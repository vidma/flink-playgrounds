package io.kensu.dam.lineage.spark.lineage

import java.util.concurrent.atomic.AtomicReference
import io.kensu.dam.model.{DataSource, DataSourcePK, PhysicalLocationRef}
import io.kensu.dam.model.DataSource

// FIMXE: we might need some kind of DS path normalized in Flink too as in Spark
final case class DatasourcePath(
  // unsanitized original path e.g. to be able to read ds for datastats using fallback
  origPath: String
) {
  lazy val qualifiedPath: String = origPath

  lazy val sanitizedQualifiedPath: String = DataSourceConv.sanitizeDsPath(qualifiedPath)

  // returns a path which could be always compared, so applies all sanitizations/normalizations
  lazy val normalizedPath: String = sanitizedQualifiedPath

  override def toString = origPath
}

object DatasourcePath {

  implicit class StringOps(s: String) {
    def toDsPath = DatasourcePath(s)
  }

  implicit class MapKeyedByStringOps[V](m: Map[String, V]) {

    def withNormalizedDsPathKey: Map[String, V] = m.map {
      case (k, v) => k.toDsPath.normalizedPath -> v
    }
  }
}

final case class OverridenDatasourceInfo(name: String, format: Option[String])

object DataSourceConv {
  val dsSanitizer = new AtomicReference[Option[(String) => String]](None)

  // Used only in tests for now, but possibly could be useful for sanitizing some sensitive info in DS paths
  def addDsPathSanitizer(searchPattern: String, replacement: String): Unit = {
    import io.kensu.dam.utils.AtomicRefUtils.AtomicRefUpdateHelper
    val fn = (x: String) => {
      searchPattern
        .split("\\|")
        .scanLeft(x)((str: String, search_item: String) => str.replace(search_item, replacement))
        .last
    }
    dsSanitizer.atomicallyUpdate(_ => Some(fn))
  }

  def sanitizeDsPath(path: String): String = {
    val dsPathTransformerFn = dsSanitizer.get().getOrElse((x: String) => x)
    // FIXME: find a better place for sanitizeJdbcUri?
    sanitizeJdbcUri(dsPathTransformerFn(path))
  }

  // What it does:
  // - remove 'jdbc:' prefix
  // - remove all jdbc options (coming after ? in connectionUri)
  // - normalize the jdbc uri string, report in a consistent way in like this protocol://connUrl/db.table
  // Here are the possible original jdbc uri values for our test inputs [returned by spline]
  // - jdbc:mysql://0.tcp.eu.ngrok.io:10155/:kensu_dam.kensu_tablename
  // - jdbc:mysql://0.tcp.eu.ngrok.io:10155/?useSSL=false&serverTimezone=UTC:kensu_dam.kensu_tablename
  // - jdbc:mysql://0.tcp.eu.ngrok.io:10155/kensu_dam:kensu_tablename
  // - jdbc:mysql://0.tcp.eu.ngrok.io:10155/kensu_dam?useSSL=false&serverTimezone=UTC:kensu_tablename
  // - jdbc:mysql://0.tcp.eu.ngrok.io:10155:kensu_dam.kensu_tablename
  // for all of then we want the final result to be: mysql://0.tcp.eu.ngrok.io:10155/kensu_dam.kensu_tablename
  // FIXME: normalize for optional (non-provided vs provided) default ports for certain protocol, so lineage would always match!!!
  def sanitizeJdbcUri(uri: String): String =
    if (uri.contains("jdbc:")) {
      // println("jdbc-uri:" + uri)
      // spline always return it as jdbc-connection-url:maybeDb-and-table, so we can split on the last : character
      uri.reverse.split(":", 2).toList.map(_.reverse) match {
        case dbAndTable :: connUrl :: Nil =>
          val connUrlWithoutOptions = connUrl.takeWhile(_ != '?').stripPrefix("jdbc:")
          val sep                   = if (dbAndTable.contains(".")) "/" else "."
          Seq(connUrlWithoutOptions.stripSuffix("/"), dbAndTable).mkString(sep)
        case _                            => uri
      }
    } else uri

  def toDataSource(
    dsPath: DatasourcePath,
    dsInfoOverrides: Map[String, OverridenDatasourceInfo] = Map.empty,
    defaultFormat: Option[String]                         = None,
    isH2oModel: Boolean                                   = false
  )(implicit environmentProvider: EnvironnementProvider, unknownLocation: PhysicalLocationRef): DataSource = {
    val origDsPath      = dsPath.origPath
    val overridenDsInfo = dsInfoOverrides.get(origDsPath)
    DataSourceConv.toDataSourceInternal(
      path              = dsPath.sanitizedQualifiedPath,
      format            = overridenDsInfo
        .flatMap(_.format)
        .orElse(DatasourceFormatOverrides.maybeOverridenValue(origDsPath))
        .orElse(defaultFormat),
      maybeExplicitName = DatasourceNameOverrides.maybeOverridenValue(origDsPath).orElse(overridenDsInfo.map(_.name)),
      isH2oModel        = isH2oModel
//        || H2oCustomSparkInputResolver.modelFilePaths.contains(origDsPath)
    )
  }

  private def toDataSourceInternal(
    path: String,
    format: Option[String] = None,
    isH2oModel: Boolean    = false,
    maybeExplicitName: Option[String]
  )(implicit environmentProvider: EnvironnementProvider, unknownLocation: PhysicalLocationRef): DataSource = {
    val ldsCategories = environmentProvider.getDatasourceLogicalName(path)
    DataSource(
      name       = DatasourceNameOverrides
        .maybeOverridenValue(path)
        .getOrElse(getDefaultDatasourceName(path, isH2oModel = isH2oModel, maybeExplicitName = maybeExplicitName)),
      format     = getDatasourceFormat(path, format, isH2oModel),
      categories = if (ldsCategories.nonEmpty) Some(ldsCategories) else None,
      pk         = DataSourcePK(path, unknownLocation)
    )
  }

  private def getDatasourceFormat(path: String, extractedFormat: Option[String], isH2oModel: Boolean) =
    DatasourceFormatOverrides
      .maybeOverridenValue(path)
      .orElse {
        extractedFormat match {
          case Some("jdbc") if path.contains("://") =>
            path.split("://").toList match {
              case jdbcConnType :: uri :: Nil =>
                Some(jdbcConnType)
              case _                          => extractedFormat
            }
          case _                                    => extractedFormat
        }
      }
      .orElse(defaultDatasourceFormat(path, isH2oModel = isH2oModel))

  def getDefaultDatasourceName(path: String, isH2oModel: Boolean = false, maybeExplicitName: Option[String])(
    implicit environmentProvider: EnvironnementProvider
  ): String =
    maybeExplicitName match {
      case Some(explicitName) => explicitName
      case None               =>
        if (!environmentProvider.useShortDatasourceNames) path else shortenDsPath(path, isH2oModel)
    }

  def getFileName(path: String): String =
    path.reverse.takeWhile(_ != '/').reverse.toString

  def shortenDsPath(path: String, isH2oModel: Boolean)(implicit environmentProvider: EnvironnementProvider): String =
    path match {
//      case x if x.startsWith(DamFakeInMemDatasource.IN_MEM_PREFIX) =>
//        path
      case _ if isH2oModel =>
        getFolderAndFileName(path)
      case _               =>
        DatasourceNamingStrategy
          .convert(environmentProvider.datasourceShortNameStrategy, path, environmentProvider.shortDsNamePathBasedRules)
          .getOrElse(getFileName(path))
    }

  def getFolderAndFileName(path: String, allowedSlashes: Int = 1): String = {
    var foundSlashes = 0
    path.reverse.takeWhile { c =>
      if (c == '/') {
        foundSlashes += 1
      }
      foundSlashes <= allowedSlashes
    }.reverse
  }

  def getFolderName(path: String): Option[String] = {
    val fileName    = getFileName(path)
    val maybeFolder = getFolderAndFileName(path)
      .replace("/" + fileName, "")
      .replace(fileName, "")
    Option(maybeFolder).filter(_.nonEmpty)
  }

  def validateGuessedDsFormat(format: String) =
    if (format.contains("/")) { // most likely part of file and not format
      None
    } else Some(format)

  def defaultDatasourceFormat(path: String, isH2oModel: Boolean = false): Option[String] =
    if (path.contains(".")) {
      Some(path.reverse.takeWhile(_ != '.').reverse.toString)
        .flatMap(validateGuessedDsFormat)
    } else None
}
