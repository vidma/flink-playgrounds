package org.apache.flink.integration.kensu

import io.kensu.dam.lineage.spark.lineage.DatasourcePath.StringOps
import io.kensu.dam.lineage.spark.lineage.{DataSourceConv, EnvironnementProvider, Lineage}
import io.kensu.dam.model.ModelHelpers.{
  CodeVersionHelper,
  DataSourceHelper,
  LineageRunHelper,
  ProcessHelper,
  ProcessLineageHelper,
  ProcessRunHelper,
  SchemaHelper,
  UserHelper
}
import io.kensu.dam.model.{
  BatchEntityReportProxy,
  CodeVersionRef,
  DataCatalogEntry,
  DataSource,
  DataStatsPK,
  FieldDef,
  LineageDef,
  LineageRun,
  LineageRunPK,
  PhysicalLocationRef,
  Process,
  ProcessCatatalogEntry,
  ProcessLineageRef,
  ProcessPK,
  ProcessRef,
  ProcessRun,
  ProcessRunPK,
  ProcessRunRef,
  ProjectRef,
  Schema,
  SchemaPK,
  UserRef
}
import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.integration.kensu.AggrEntry.toKensuStatName
import org.apache.flink.integration.kensu.KensuFlinkHook.{logDebug, logInfo, logVarWithType}
import org.apache.flink.integration.kensu.KensuStatsContext.ComputeStatsFn
import org.apache.flink.integration.kensu.KensuStatsHelpers.extractTableImpl
import org.apache.flink.integration.kensu.StatConstants._
import org.apache.flink.integration.kensu.StatsInputsExtractor.extractTableInputs
import org.apache.flink.integration.kensu.connectors.KafkaConn
import org.apache.flink.integration.kensu.reflect.ReflectionHelpers.reflOrThrow
import org.apache.flink.integration.kensu.sinks.ingestion.DAMClientFactory
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.Expressions.{$, lit}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal, TableImpl}
import org.apache.flink.table.api.{
  ApiExpression,
  Table,
  TableColumn,
  TableEnvironment,
  TableNotExistException,
  TableResult,
  Tumble,
  TumbleWithSize,
  TumbleWithSizeOnTime
}
import org.apache.flink.table.catalog.exceptions.CatalogException
import org.apache.flink.table.catalog.{
  Catalog,
  CatalogBaseTable,
  Column,
  ObjectIdentifier,
  ResolvedCatalogTable,
  ResolvedSchema,
  UnresolvedIdentifier
}
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.expressions.{Expression, ResolvedExpression}
import org.apache.flink.table.operations.{CatalogQueryOperation, ProjectQueryOperation, QueryOperation}
import org.apache.flink.table.operations.utils.QueryOperationDefaultVisitor
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, LogicalType, TimestampType, VarCharType}
import org.apache.flink.table.types.{AtomicDataType, CollectionDataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.util
import java.util.{List, UUID}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

object StatConstants {
  val NUMERIC_DISTINT_STAT = "distinct_values"

  val ksuDataPrefix          = "group_"
  val Stats_Window_Alias     = ksuDataPrefix + "statsWindow"
  val Stats_Window_End_Alias = ksuDataPrefix + "stats_Window_end"

  val Ds_Path_Alias         = ksuDataPrefix + "Ksu_Ds_Path_Alias"
  val Ds_SchemaId_Alias     = ksuDataPrefix + "Ksu_Ds_Schema_Id_Alias"
  val Ds_ProcessRunId_Alias = ksuDataPrefix + "DS_ProcessRunId_Alias"
  val Ds_LineageId_Alias    = ksuDataPrefix + "Ds_LineageId_Alias"
}

final case class AggrEntry(
  expr: ApiExpression,
  flinkUniqueName: String,
  dType: String
) {
  def getAggrExpr = expr.as(flinkUniqueName)
}

object AggrEntry {

  def fmtStatFieldname(column: String, stat: String) =
    s"${column}_ksu_${stat}"

  // special case: _ksu_nrows => nrows (without column prefix)
  @inline
  def toKensuStatName(fieldName: String) = fieldName
    .replace("_ksu_", ".")
    .stripPrefix(".")

  def apply(expr: ApiExpression, column: String, stat: String, dataType: LogicalType): AggrEntry =
    AggrEntry(
      expr            = expr,
      flinkUniqueName = fmtStatFieldname(column, stat),
      dType           = dataType.asSerializableString()
    )

}

case class KsuDataCatalogEntry(
  ds: DataSource,
  fields: Seq[FieldDef],
  statsContext: Option[KensuStatsContext] = None
) extends DataCatalogEntry {
  override lazy val damDataSource = ds

  val location: String       = ds.pk.location
  val format: Option[String] = ds.format
  val physicalLocationRef    = ds.pk.physicalLocationRef

  def enqueToBatch()(implicit enquedEntitiesRef: AtomicReference[BatchEntityReportProxy]): Unit = {
    ds.enqueToBatch()
    damSchema.enqueToBatch()
  }

  def withStatsCtx(statsContext: KensuStatsContext): KsuDataCatalogEntry =
    copy(statsContext = Some(statsContext))
}

object KensuStatsContext {
  // lineageId: String, processRunId: String
  type ComputeStatsFn = (String, String) => (String, Table)
}

case class KensuStatsContext(
  timeWindowGroupExpression: TumbleWithSizeOnTime,
  countDistinctCols: Array[String] = Array.empty,
  // lineageId: String, processRunId: String => Unit
  computeStatsFn: ComputeStatsFn
)

// FIXME: Flink might not know what to do with it!!! so for now we require filter to be present
class KensuStatsProjectQueryOperation(
  val underlyingProject: ProjectQueryOperation,
  val projectReflect: ProjectQueryOperationReflectOps,
  val kensuStatsContext: KensuStatsContext
) extends ProjectQueryOperation(
    // these are private fields, so need reflection to extract them
    projectReflect.projectList,
    projectReflect.child,
    projectReflect.resolvedSchema
  ) {}

class ProjectQueryOperationReflectOps(p: ProjectQueryOperation) {
  def projectList: util.List[ResolvedExpression] = reflOrThrow[util.List[ResolvedExpression]](p, "projectList")
  def child: QueryOperation                      = reflOrThrow[QueryOperation](p, "child")
  def resolvedSchema: ResolvedSchema             = reflOrThrow[ResolvedSchema](p, "resolvedSchema")
}

object StatsInputsExtractor {
  // FIXME: for now might behave weird, if input is multiple DSes with complex transformations

  def nullSafe(s: String): String =
    Option(s).getOrElse("[null]")

  def nullSafeAny(s: Any): String =
    Option(s).map(_.toString).getOrElse("[null]")

  def extractInputsFromQueryOperation(
    tEnv: TableEnvironment,
    queryOperation: QueryOperation
  )(implicit envProvider: EnvironnementProvider, dsLocation: PhysicalLocationRef): Seq[KsuDataCatalogEntry] = {

    var inputs  = Seq.empty[KsuDataCatalogEntry]
    val visitor = new QueryOperationDefaultVisitor[Unit]() {
      override def defaultMethod(queryOperation: QueryOperation): Unit = {
        logVarWithType(s"Visiting QueryOperation:", queryOperation)
        val childrenNodes = queryOperation.getChildren.asScala
        childrenNodes.foreach(c => c.accept(this))
      }

      override def visit(other: ProjectQueryOperation): Unit =
        other match {
          case op: KensuStatsProjectQueryOperation =>
            logInfo(s"found KensuStatsProjectQueryOperation from markStatsInput, statsContext=${op.kensuStatsContext}")
            val projectInputs = extractInputsFromQueryOperation(tEnv, op.underlyingProject)
              .map(_.withStatsCtx(op.kensuStatsContext))
            inputs ++= projectInputs
          case _                                   =>
            super.visit(other)
        }

      override def visit(catalogTable: CatalogQueryOperation): Unit = {
        val tableId: ObjectIdentifier = catalogTable.getTableIdentifier
        val tableIdStr                = tableId.asSerializableString
        logInfo(s"found CatalogQueryOperation: $tableIdStr")
        // e.g. `default_catalog`.`default_database`.`transactions`
        // it is unresolved input ID at this stage, so try to resolve it
        inputs ++= catalogTableToKensu(tEnv, tableId)
      }
    }
    queryOperation.accept(visitor)
    inputs
  }

  def extractTableInputs(t: TableImpl)(
    implicit envProvider: EnvironnementProvider,
    dsLocation: PhysicalLocationRef
  ): Seq[KsuDataCatalogEntry] = {
    val tEnv: TableEnvironment = t.getTableEnvironment
    logInfo("TableImpl.explain:")
    t.explain()
    val inputs                 = extractInputsFromQueryOperation(tEnv, t.getQueryOperation)
    logInfo(s"resolved input paths for stats input: ${inputs}")
    inputs
  }

  def extractCatalogTable(tEnv: TableEnvironment, tableId: ObjectIdentifier) =
    for {
      cat                     <- tEnv.getCatalog(tableId.getCatalogName).asScala
      table: CatalogBaseTable <-
        Try(cat.getTable(tableId.toObjectPath))
          .recoverWith { case NonFatal(ex) => logInfo(s"unable to get table info: $tableId, ex: ${ex}"); Failure(ex) }
          .toOption
    } yield {
      logVarWithType("table", table)
      val tableOptions = table.getOptions.asScala
      logInfo(
        s"""
           |table.toString: ${table.toString}
           |table.getOptions: ${tableOptions}
           |""".stripMargin
      )
      (table, tableOptions)
    }

  implicit class ResolvedSchemaOps(s: ResolvedSchema) {

    def toKensuSchemaFields: Seq[FieldDef] =
      s.getColumns.asScala
        .filter(_.isPhysical)
        .map { c =>
          FieldDef(
            name      = c.getName,
            fieldType = c.getDataType.getLogicalType.asSummaryString(),
            nullable  = c.getDataType.getLogicalType.isNullable
          )
        }.toSeq
  }

  implicit class ResolvedCatalogTableOps(t: ResolvedCatalogTable) {

    def toKensuCatalogEntry(
      dsPath: String,
      dsFormat: Option[String]
    )(implicit environmentProvider: EnvironnementProvider, unknownLocation: PhysicalLocationRef) = {
      val ds                          = DataSourceConv.toDataSource(dsPath = dsPath.toDsPath, defaultFormat = dsFormat)
      val schemaFields: Seq[FieldDef] = t.getResolvedSchema.toKensuSchemaFields
      logInfo(s"$dsFormat: path='$dsPath'  schema=[${t.getResolvedSchema.toKensuSchemaFields}]")
      KsuDataCatalogEntry(ds = ds, fields = schemaFields)
    }
  }

  def catalogTableToKensu(tEnv: TableEnvironment, tableId: ObjectIdentifier)(
    implicit environmentProvider: EnvironnementProvider,
    unknownLocation: PhysicalLocationRef
  ): Option[KsuDataCatalogEntry] = {
    for {
      (table, tOpts) <- extractCatalogTable(tEnv, tableId)
    } yield {
      table match {
        // table.getOptions: Map(properties.bootstrap.servers -> kafka:9092, format -> transactions, connector -> kafka)
        case t: ResolvedCatalogTable if tOpts.get("connector").contains("kafka") =>
          logInfo(s"Kafka: schema [${t.getResolvedSchema.toKensuSchemaFields}]")
          (tOpts.get("topic"), tOpts.get("properties.bootstrap.servers")) match {
            case (Some(topic), Some(uri)) =>
              val dsPath = KafkaConn.toKensuDatasourceLocation(topic = topic, kafkaUri = uri)
              Some(t.toKensuCatalogEntry(dsPath = dsPath, dsFormat = Some("Kafka")))
            case _                        =>
              None
          }
        case t: ResolvedCatalogTable if tOpts.get("connector").contains("jdbc")  =>
          logInfo(s"JDBC: schema [${t.getResolvedSchema.toKensuSchemaFields}]")
          // "  'connector'  = 'jdbc',\n" +
          //          "  'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
          //            "  'table-name' = 'spend_report',\n" +
          //            "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
          (tOpts.get("url"), tOpts.get("table-name"), tOpts.get("driver")) match {
            case (Some(url), Some(tableName), Some("com.mysql.jdbc.Driver")) =>
              // FIXME: do some smarter URL processing
              val qualifiedUrl = Seq(
                url.stripPrefix("jdbc:").stripSuffix("/"),
                tableName
              ).mkString("/")
              Some(t.toKensuCatalogEntry(dsPath = qualifiedUrl, dsFormat = Some("MySQL")))
            case _                                                           =>
              None
          }
      }
    }
  }.flatten
}

object KensuStatsHelpers {

  def kensuExecuteInsert(
    tbl: Table,
    tablePath: String,
    overwrite: Boolean                                   = false,
    outStatsTimeWindowExpr: Option[TumbleWithSizeOnTime] = None,
    outStatsCountDistinctCols: Array[String]             = Array.empty
  ): TableResult =
    tbl.kensuExecuteInsert(tablePath, overwrite, outStatsTimeWindowExpr, outStatsCountDistinctCols)

  implicit class TableOps(tbl: Table) {

    def kensuMarkStatsInput(
      timeWindowGroupExpression: TumbleWithSizeOnTime,
      countDistinctCols: Array[String] = Array.empty
    ): Table =
      KensuStatsHelpers.markStatsInput(
        timeWindowGroupExpression = timeWindowGroupExpression,
        countDistinctCols         = countDistinctCols,
        table                     = tbl
      )

    protected def resolveTableIdentifier(tblImpl: TableImpl, tablePath: String): ObjectIdentifier = {
      // based on Table.executeInsert() from flink
      val tblEnv               = tblImpl.getTableEnvironment.asInstanceOf[TableEnvironmentInternal]
      val unresolvedIdentifier = tblEnv.getParser.parseIdentifier(tablePath)
      tblEnv.getCatalogManager.qualifyIdentifier(unresolvedIdentifier)
    }

    private def kensuLogErrorHandler[U]: PartialFunction[Throwable, U] = {
      case NonFatal(ex) =>
        logInfo("exception in kensuExecuteInsert: " + ex.toString)
        ex.printStackTrace()
        throw ex
    }

    def kensuExecuteInsert(
      tablePath: String,
      overwrite: Boolean                                   = false,
      outStatsTimeWindowExpr: Option[TumbleWithSizeOnTime] = None,
      outStatsCountDistinctCols: Array[String]             = Array.empty
    ): TableResult = {
      val tblEnv  = extractTableImpl(tbl).map { tblImpl: TableImpl =>
        tblImpl.getTableEnvironment
      }.get
      val stmtSet = tblEnv.createStatementSet
      stmtSet.addInsert(tablePath, tbl, overwrite)

      val stmtResult = Try {
        extractTableImpl(tbl).map { tblImpl: TableImpl =>
          val tblEnv                               = tblImpl.getTableEnvironment
          // FIXME: this is wrong way to get conf:
          // Got Flink conf: Map(execution.runtime-mode -> STREAMING, table.planner -> BLINK)
          val flinkConf: Configuration             = GlobalConfiguration.loadConfiguration() // FIXME: doesn't seem to always work?
          logDebug(s"Got Flink conf: ${flinkConf.toMap.asScala}")
          @transient lazy val kensuClient          = DAMClientFactory.getDAMClient(flinkConf)
          @transient lazy val kensuApi             = kensuClient.toBatchContainer
          @transient implicit lazy val envProvider = EnvironnementProvider.create(flinkConf, kensuApi)
          import kensuApi.Implicits._
          val inputs                               = StatsInputsExtractor.extractTableInputs(tblImpl)
          val outTableId                           = resolveTableIdentifier(tblImpl, tablePath)
          val maybeOutput                          = StatsInputsExtractor.catalogTableToKensu(tblImpl.getTableEnvironment, outTableId)
          logInfo(
            s"""======
               |kensuExecuteInsert:
               |------
               |inputs: ${inputs}
               |output: ${maybeOutput}
               |------
               |""".stripMargin
          )
          maybeOutput.map { output =>
            import org.apache.flink.table.api.StatementSet

            envProvider.explicitProcessRunName

            val defaultJobName = "Flink :: insert-into_" + outTableId
            val launcher       = envProvider.getUser
            val codeVersion    = envProvider.getCodeVersion
            val processName    =
              envProvider.explicitProcessName.getOrElse(
                defaultJobName
              ) // App name is often too ambiguous, as people don't bother to set it

            val appId          = s"$processName at ${java.time.Instant.now().toString}"
            val processRunName =
              envProvider.explicitProcessRunName.getOrElse(
                appId
              ) // App name is often too ambiguous, as people don't bother to set it
            val process        = Process(ProcessPK(processName))
            val processRun     = ProcessRun(
              pk                     = ProcessRunPK(process.toRef, processRunName),
              launchedByUserRef      = launcher.map(_.toRef),
              executedCodeVersionRef = codeVersion.map(_.toRef),
              environment            = envProvider.getRuntimeEnvironment,
              projectsRefs           = envProvider.getProjects(defaultProject = Some(processName))
            )
            val lin            =
              Lineage.approx(process.toRef, inputSchemas = inputs.map(_.damSchema), outputSchema = output.damSchema)
            inputs.foreach(_.enqueToBatch())
            output.enqueToBatch()
            process.enqueToBatch()
            processRun.enqueToBatch()
            lin.enqueToBatch()
            val linRun         = LineageRun(
              pk = LineageRunPK(
                processRunRef = processRun.toRef,
                lineageRef    = lin.toRef,
                /* The timestamp at which the lineage was observed */
                timestamp     = System.currentTimeMillis()
              )
            )
            linRun.enqueToBatch()

            // submit job(s) to report DataStats of inputs
            val lineageGuid = lin.pk.guid
            inputs.foreach { input =>
              Try {
                input.statsContext.foreach { x =>
                  val (outstatsKsuTblId, aggrTable) = x.computeStatsFn(lineageGuid, processRun.pk.guid)
                  stmtSet.addInsert(outstatsKsuTblId, aggrTable)
                }
              }.recover(kensuLogErrorHandler)
            }
            // datastats of output(s)
            // FIXME: currently do not work due to limitation of Flink v0.13...
            val stmtResult  = Try {
              outStatsTimeWindowExpr match {
                case Some(outStatsTimeWindowExpr) =>
                  val outstatsFn                    = buildStatsStreamFn(
                    timeWindowGroupExpression = outStatsTimeWindowExpr,
                    countDistinctCols         = outStatsCountDistinctCols,
                    table                     = tbl,
                    tEnv                      = tblImpl.getTableEnvironment,
                    kensuDs                   = output,
                    jobPrefix                 = "output_"
                  )
                  // FIXME: out stats do not work...
                  logInfo(s"starting output stats stream for ${output}...")
                  val (outstatsKsuTblId, aggrTable) = outstatsFn(lineageGuid, processRun.pk.guid)
                  stmtSet.addInsert(outstatsKsuTblId, aggrTable)
                  logInfo(s"started output stats stream for ${output}.")
                case _                            =>
                  logInfo(s"output stats for ${output} not requested. skipping...")
              }
            }.recover(kensuLogErrorHandler)

            // FIXME: stats fails otherwise if reported earlier on?? ?
            Try {
              val future = kensuApi.reportToDamAndLogErrorNonBlocking(thowErr = false)
              Await.result(future, 30.seconds)
            }.recover(kensuLogErrorHandler)

            stmtResult
          }
        // FIXME: make this concurrent?
        }
      }.recover(kensuLogErrorHandler)
      // FIXME: validate and start original job first?

      // FIXME: better handling of Kensu issues
//      stmtResult.map(_.flatten.get).flatten.get
////      val originalResult = tbl.executeInsert(tablePath, overwrite)
////      originalResult
      Try(stmtSet.explain())
      stmtSet.execute()
    }

  }

  private def extractTableImpl(t: Table): Option[TableImpl] =
    t match {
      case tableImpl: TableImpl => Some(tableImpl)
      case _                    =>
        logInfo(s"markStatsInput: not supported for flink Table which is not TableImpl")
        None
    }

  def buildStatsStreamFn(
    timeWindowGroupExpression: TumbleWithSizeOnTime,
    countDistinctCols: Array[String],
    table: Table,
    tEnv: TableEnvironment,
    kensuDs: KsuDataCatalogEntry,
    jobPrefix: String
  ): ComputeStatsFn = {
    // how to get original input path automatically?
    val inputId: String = jobPrefix + "stat" + UUID.randomUUID().toString.replace("-", "")
    import scala.collection.JavaConverters._
    // based on Table fromResolvedSchema
    val tableSchema     = table.getResolvedSchema.getColumns.asScala

    val bigIntType = new BigIntType(false)
    val doubleType = new DoubleType(false)

    val statAggregations: Seq[AggrEntry]     = table.getResolvedSchema.getColumns.asScala.collect {
      case c: Column.PhysicalColumn =>
        val colName     = c.getName
        val colDataType = c.getDataType
        // found [PhysicalColumn]: name=account_id dataType: BIGINT dataTypeCls: org.apache.flink.table.types.AtomicDataType`account_id` BIGINT
        // found [PhysicalColumn]: name=transaction_time dataType: TIMESTAMP(3) *ROWTIME* dataTypeCls: org.apache.flink.table.types.AtomicDataType`transaction_time` TIMESTAMP(3) *ROWTIME*
        // found [PhysicalColumn]: name=amount dataType: BIGINT dataTypeCls: org.apache.flink.table.types.AtomicDataType`amount` BIGINT
        logInfo(
          s"markStatsInput: found [PhysicalColumn]: name=${colName} dataType: ${colDataType} dataTypeCls: ${colDataType.getClass.getCanonicalName}" + c
        )
        colDataType.getLogicalType match {
          case logicalType @ (_: TimestampType | _: BigIntType) =>
            val numericOnlyAggr = logicalType match {
              case x: TimestampType => Seq.empty
              case _                => Seq(
                  AggrEntry($(colName).avg(), colName, "mean", doubleType) // .avg() has double type by default
                  // fixme: we could probably have it... just need to cast?
                )
            }
            Seq(
              AggrEntry($(colName).isNull.count(), colName, "nullrows", bigIntType),
              AggrEntry($(colName).min(), colName, "min", logicalType),
              AggrEntry($(colName).max(), colName, "max", logicalType)
            ) ++ numericOnlyAggr

          case logicalType @ (_: VarCharType) =>
            Seq(
              AggrEntry($(colName).isNull.count(), colName, "nullrows", bigIntType)
            )

          // FIMXE: add/test STRING datatypes
          case _ => Seq.empty
        }

      case cxx: Column.MetadataColumn =>
        logInfo("markStatsInput: input Table contains column of unsupported column type [MetadataColumn]: " + cxx)
        Seq.empty
      case cx: Column.ComputedColumn  =>
        // TableColumn.computed(cx.getName, cx.getDataType, cx.getExpression.asSerializableString)
        logInfo("markStatsInput: input Table contains column of unsupported column type [ComputedColumn]: " + cx)
        Seq.empty
      case column                     =>
        logInfo("markStatsInput: input Table contains column of unsupported column type: " + column)
        Seq.empty
    }.flatten
    val distinctAggregations: Seq[AggrEntry] = countDistinctCols.flatMap { distinctColName =>
      tableSchema.find(_.getName == distinctColName).map(_.getDataType.getLogicalType).map { dType =>
        // jobmanager_1      | Caused by: org.apache.flink.table.api.ValidationException: Distinct modifier cannot be applied to 'account_id! It can only be applied to an aggregation expression, for example, 'a.count.distinct which is equivalent with COUNT(DISTINCT a).
        AggrEntry($(distinctColName).count().distinct(), distinctColName, NUMERIC_DISTINT_STAT, bigIntType)
      }
    }
    val nrows                                = Seq(
      AggrEntry(lit(1L, new AtomicDataType(bigIntType)).sum(), "", "nrows", bigIntType)
    )

    // FIXME: can we get tEnv automagically?
    def computeStatsFn(lineageId: String, processRunId: String): (String, Table) = {

      val groupColums                                   = Seq($(Stats_Window_Alias))
      val windowEndExpr                                 = $(Stats_Window_Alias).end()
      val varCharType                                   = new VarCharType(false, VarCharType.MAX_LENGTH)
      val inputSchemaId                                 = kensuDs.damSchema.pk.guid
      val dsPath                                        = kensuDs.damDataSource.pk.location
      def strLiteral(colName: String, litValue: String) =
        AggrEntry(lit(litValue, new AtomicDataType(varCharType)), colName, varCharType.asSerializableString)

      val dsIdentifierColumns = Seq(
        // output.damSchema.pk.guid, lin.pk.guid, processRun.pk.guid
        strLiteral(Ds_Path_Alias, dsPath),
        strLiteral(Ds_SchemaId_Alias, inputSchemaId),
        strLiteral(Ds_ProcessRunId_Alias, processRunId),
        strLiteral(Ds_LineageId_Alias, lineageId),
        AggrEntry(windowEndExpr, Stats_Window_End_Alias, (new TimestampType).asSerializableString)
      )
      val allAggregations     = distinctAggregations ++ statAggregations ++ nrows ++ dsIdentifierColumns
      logInfo(allAggregations.toString())

      val printTableId  = s"${inputId}"
      val printTableSql = s"""CREATE TABLE ${printTableId} (
                             |  ${allAggregations.map(a => s"${a.flinkUniqueName} ${a.dType}").mkString(",\n")}
                             |) WITH (
                             |  'connector' = 'kensu-stats-reporter'
                             |)""".stripMargin

      logInfo("statsWritterTableSQL:" + printTableSql)
      tEnv.executeSql(printTableSql)

      // FIXME: this kind of output stats would not work until upgrading to Flink 0.14
      // org.apache.flink.table.api.TableException: StreamPhysicalGroupWindowAggregate doesn't support consuming
      // update changes which is produced by node
      // GroupAggregate(groupBy=[account_id, log_ts], select=[account_id, log_ts, SUM(amount) AS EXPR$0])
      printTableId -> table
        // https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-tvf/
        // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/tableapi/#overview--examples
        .window(timeWindowGroupExpression.as(Stats_Window_Alias))
        .groupBy(groupColums: _*)
        .select(
          allAggregations.map(_.getAggrExpr): _*
        )
      // .executeInsert(printTableId)
    }
    computeStatsFn _
  }

  def markStatsInput(
    timeWindowGroupExpression: TumbleWithSizeOnTime,
    countDistinctCols: Array[String],
    table: Table
  ): Table = {
    val tableImpl                            = extractTableImpl(table)
      .getOrElse(return table)
    val tEnv                                 = tableImpl.getTableEnvironment
    val flinkConf                            = tableImpl.getTableEnvironment.getConfig.getConfiguration // FIXME: doesn't seem to always work?
    @transient lazy val kensuClient          = DAMClientFactory.getDAMClient(flinkConf)
    @transient lazy val kensuApi             = kensuClient.toBatchContainer
    @transient implicit lazy val envProvider = EnvironnementProvider.create(flinkConf, kensuApi)
    import kensuApi.Implicits._
    val inputDs: KsuDataCatalogEntry         = extractTableInputs(tableImpl).toList match {
      case inputDs :: Nil => inputDs
      case path1 :: xs    =>
        logInfo(s"markStatsInput: multiple inputs for stats Table not supported")
        return table
      case Nil            =>
        logInfo(s"markStatsInput: failed automatically extract input datasource name (not supported connector?)")
        return table
    }
    val computeStatsFn                       = buildStatsStreamFn(
      timeWindowGroupExpression: TumbleWithSizeOnTime,
      countDistinctCols = countDistinctCols,
      table             = table,
      tEnv              = tEnv,
      kensuDs           = inputDs,
      jobPrefix         = "input_"
    )
    kensuWrapStatsOperation(
      statsContext = KensuStatsContext(
        timeWindowGroupExpression = timeWindowGroupExpression,
        countDistinctCols         = countDistinctCols,
        computeStatsFn            = computeStatsFn
      ),
      origTable = tableImpl
    )
  }

  protected def kensuWrapStatsOperation(statsContext: KensuStatsContext, origTable: TableImpl): TableImpl = {
    val projectOp: ProjectQueryOperation = origTable.getQueryOperation match {
      case p: ProjectQueryOperation =>
        p
      case _                        =>
        logInfo("markStatsInput: called with a Table param which is not a .select() operation - " +
          "unable to mark/extract datastats - not supported yet")
        return origTable
    }

    callCreateTable(
      origTable,
      new KensuStatsProjectQueryOperation(
        underlyingProject = projectOp,
        projectReflect    = new ProjectQueryOperationReflectOps(projectOp),
        kensuStatsContext = statsContext
      )
    )
  }

  private def callCreateTable(origTable: TableImpl, operation: QueryOperation): TableImpl = {
    val obj    = origTable
    // see https://stackoverflow.com/a/161005
    // P.S. obj.getClass.getMethod is only for public methods
    val method = obj.getClass.getDeclaredMethod("createTable", classOf[QueryOperation]);
    method.setAccessible(true)
    method.invoke(obj, operation).asInstanceOf[TableImpl]
  }

  @inline
  def convertStatToKensu(fieldName: String, fieldValue: Any): (String, Option[Double]) = {
    val kensuStatName  = toKensuStatName(fieldName)
    // FIXME: convert timestamp to int!!!??
    val kensuStatValue = fieldValue match {
      case t: TimestampData    => Some(t.getMillisecond.toDouble)
      case t: java.lang.Long   => Some(t.toDouble)
      case t: java.lang.Double => Some(t.toDouble)
      case _                   =>
        logVarWithType(s"Unsupported stat data value for field '$fieldName':", fieldValue)
        None
    }
    kensuStatName -> kensuStatValue
  }
}
