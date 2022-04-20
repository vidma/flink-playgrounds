package io.kensu.dam.lineage.spark.lineage

import java.util.concurrent.atomic.AtomicReference
import akka.actor.ActorSystem
import io.kensu.dam._
import io.kensu.dam.lineage.spark.lineage.BatchDebugger.BatchDebuggerImplicits
import io.kensu.dam.lineage.spark.lineage.DatasourceNamingStrategy.{
  DatasourceNamingStrategy,
  REGEXP_MATCH_PREFIX,
  REGEXP_REPLACE_PREFIX
}
import io.kensu.dam.lineage.spark.lineage.DefaultEntities.unknownLocation
import io.kensu.dam.model._
import io.kensu.dam.model.ModelHelpers._
import io.kensu.dam.model.CacheKeysImplicits._
import io.kensu.lineage.utils.ConfKeys.ColumnLineageFallbackStrategy
import io.kensu.lineage.utils.{ConfKeys, DamLogging, FlinkConfExtractor, TimeUtils}
import io.swagger.client.core.{ApiInvoker, OfflineApiInvoker}
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.integration.kensu.sinks.ingestion.DamClientActorSystem

import java.util.regex.Pattern
import scala.concurrent.duration.Duration
import scala.concurrent.{duration, Await, ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

object DefaultEntities {
  val dummyDouble     = 123456789.123456789
  val unknownLocation = PhysicalLocation("unknown", dummyDouble, dummyDouble, PhysicalLocationPK("unknown", "unknown"))
}

final case class DamEntitiesApiWrapper(
  apiInvoker: ApiInvoker,
  api: ManageKensuDAMEntitiesApi,
  executionContext: ExecutionContext,
  actorSystem: ActorSystem
) {

  object Implicits {
    implicit val implicitDamApi      = api
    implicit val implicitEc          = executionContext
    implicit val implicitActorSystem = actorSystem
  }

  def toBatchContainer: BatchDamEntitiesApiWrapper = {
    val newBatch =
      BatchHelpers.newBatch(timestampStrategy = () => TimeUtils.getCurrentTimestamp(needsUniqueValue = false))
    BatchDamEntitiesApiWrapper(apiInvoker, api, executionContext, actorSystem, new AtomicReference(newBatch))
  }
}

final case class BatchDamEntitiesApiWrapper(
  apiInvoker: ApiInvoker,
  api: ManageKensuDAMEntitiesApi,
  executionContext: ExecutionContext,
  actorSystem: ActorSystem,
  batchItems: AtomicReference[BatchEntityReportProxy] // = BatchHelpers.newBatchContainer
) extends DamLogging {

  object Implicits {
    implicit val implicitDamApi         = api
    implicit val implicitEc             = executionContext
    implicit val implicitActorSystem    = actorSystem
    implicit val implicitBatchItemsCont = batchItems
    implicit val unknownLocationref     = unknownLocation.enqueToBatch().toRef
  }

  def mkCopy() =
    copy(batchItems = new AtomicReference(batchItems.get().copy()))

  def empty() =
    copy(batchItems = new AtomicReference(BatchHelpers.newBatch()))

  def combineWith(other: BatchDamEntitiesApiWrapper) = {
    val combinedItems = batchItems.get().combinedWith(other.batchItems.get())
    copy(batchItems = new AtomicReference(combinedItems))
  }

  def reportToDamAndLogErrorNonBlocking(thowErr: Boolean = false): Future[Unit] = {
    import Implicits._
    val shortEntityInfo = batchItems.get().toShortOverview
    val ksuTarget       = apiInvoker match {
      case _: OfflineApiInvoker => "offline file"
      case _                    => "server(s)"
    }
    val f               = ModelHelpers
      .reportEnquedEntities()
      .recover {
        case NonFatal(err) =>
          damLogError(s"Error reporting entities to Kensu ${ksuTarget}: ${shortEntityInfo}", err)
          if (thowErr) throw err else ()
      }
      .map { batchResult =>
        damLogInfo(s"Entities reported successfully to Kensu ${ksuTarget}: ${shortEntityInfo}")
        ()
      }
    DamClientActorSystem.registerFutureToAwait(f)
    f
  }

}

object DatasourceNamingStrategy extends Enumeration {
  type DatasourceNamingStrategy = Value
  val File, LastFolderAndFile, LastTwoFoldersAndFile, LastFolder, PathBasedRule, RegexExtractorRule = Value

  val REGEXP_MATCH_PREFIX   = "regexp-match:"
  val REGEXP_REPLACE_PREFIX = "replace:"

  def applyRegexpExtraction(path: String, regexp: String, extraction: String) =
    Try(Pattern.compile(regexp).matcher(path).replaceFirst(extraction))
      // FIXME: log
      .toOption

  def convert(strategy: DatasourceNamingStrategy, path: String, namingRules: Seq[PathBasedNamingRule]): Option[String] =
    strategy match {
      case File                  => Some(DataSourceConv.getFileName(path))
      case LastFolderAndFile     => Some(DataSourceConv.getFolderAndFileName(path))
      case LastTwoFoldersAndFile => Some(DataSourceConv.getFolderAndFileName(path, allowedSlashes = 2))
      case LastFolder            => DataSourceConv.getFolderName(path)
      case PathBasedRule         =>
        namingRules
          .filterNot(_.strategy == PathBasedRule)
          .flatMap {
            case r if r.strategy == RegexExtractorRule && path.matches(r.pathMatcher) && r.extractor.isDefined =>
              applyRegexpExtraction(path = path, regexp = r.pathMatcher, extraction = r.extractor.get)
            case r if path.contains(r.pathMatcher)                                                             =>
              convert(r.strategy, path, namingRules)
            case _                                                                                             => None
          }
          .headOption
    }
}

final case class PathBasedNamingRule(
  pathMatcher: String,
  strategy: DatasourceNamingStrategy,
  extractor: Option[String] = None
)

object PathBasedNamingRule extends DamLogging {

  def regexpReplacer(matchRegexp: String, replaceRegexp: String) =
    s"${REGEXP_MATCH_PREFIX}${matchRegexp}->>${REGEXP_REPLACE_PREFIX}${replaceRegexp}"

  def parse(rule: String): Option[PathBasedNamingRule] =
    rule.split("->>").toList match {
      case matcher :: regexpExtr :: Nil
          if matcher.startsWith(REGEXP_MATCH_PREFIX) && regexpExtr.startsWith(REGEXP_REPLACE_PREFIX) =>
        val regexp    = matcher.stripPrefix(REGEXP_MATCH_PREFIX)
        val extractor = regexpExtr.stripPrefix(REGEXP_REPLACE_PREFIX)
        Some(PathBasedNamingRule(regexp, DatasourceNamingStrategy.RegexExtractorRule, extractor = Some(extractor)))
      case matcher :: value :: Nil =>
        Try(DatasourceNamingStrategy.withName(value)).recover {
          case NonFatal(err) =>
            damLogWarn(s"Unexpected naming_strategy=${value} inside PathBasedNamingRule: ${rule}")
            throw err
        }.toOption
          .map(strategy => PathBasedNamingRule(matcher, strategy))
      case _                       =>
        damLogWarn(s"Unexpected format of PathBasedNamingRule: ${rule}")
        None
    }

  def parseRuleList(ruleList: Seq[String]): Seq[PathBasedNamingRule] =
    ruleList.flatMap(s => PathBasedNamingRule.parse(s).toSeq)

}

abstract class GenericKeyValueOverrides {
  protected val valueOverrides = new AtomicReference[Map[String, String]](Map.empty)

  def maybeOverridenValue(key: String): Option[String] =
    valueOverrides.get().get(key)

  def addOverride(key: String, forcedValue: String): Unit = {
    import io.kensu.dam.utils.AtomicRefUtils.AtomicRefUpdateHelper
    valueOverrides.atomicallyUpdate(oldMap => oldMap ++ Map(key -> forcedValue))
  }
}

object DatasourceLogicalNameOverrides extends GenericKeyValueOverrides
object DatasourceNameOverrides extends GenericKeyValueOverrides
object DatasourceFormatOverrides extends GenericKeyValueOverrides

object InternalDatasourceTags extends GenericKeyValueOverrides {
  val TAG_INPUT_LINEAGE_ONLY                    = "input_lineage_only"
  def tagAsInputLineageOnly(path: String): Unit = InternalDatasourceTags.addOverride(path, TAG_INPUT_LINEAGE_ONLY)
}

object EnvironnementProvider extends DamLogging {

  def create(configuration: ReadableConfig, entitiesApiBatch: BatchDamEntitiesApiWrapper): EnvironnementProvider =
    DefaultSparkEnvironnementProvider(configuration, entitiesApiBatch)
//    val environnementProviderClassName = configuration.getString(ConfKeys.EnvironnementProviderClassName)
//    damLogWarn(s"Creating EnvironnementProvider $environnementProviderClassName")
//    val environnementProvider = Class
//      .forName(environnementProviderClassName)
//      .getConstructor(classOf[Configuration], classOf[BatchDamEntitiesApiWrapper])
//      .newInstance(configuration, entitiesApiBatch)
//      .asInstanceOf[EnvironnementProvider]
//    environnementProvider
}

trait EnvironnementProvider extends FlinkConfExtractor {

  def getUser: Option[User]
  def getCodeVersion: Option[CodeVersion]
  def getRuntimeEnvironment: Option[String]
  def getProjects(defaultProject: Option[String]): Option[List[ProjectRef]]
  def explicitProcessName: Option[String]

  lazy val autoDatastatsEnabled: Boolean =
    getOptBooleanPropOrConf(ConfKeys.AutomaticallyGatherDatastats).getOrElse(false)

  lazy val inputDiscoveryEnabled: Boolean = getOptBooleanPropOrConf(ConfKeys.GatherInputDatastats).getOrElse(false)

  lazy val inputDatastatsEnabled: Boolean = autoDatastatsEnabled && inputDiscoveryEnabled

  lazy val inputDatastatsForOnlyLineageCols: Boolean =
    getOptBooleanPropOrConf(ConfKeys.InputDatastatsOnlyLineageCols).getOrElse(true)

  lazy val inputDatastatsAllColumnsFallbackWhenNoLineage: Boolean =
    getOptBooleanPropOrConf(ConfKeys.InputDatastatsAllColumnsFallbackWhenNoLineage).getOrElse(true)

  lazy val inputDatastatsKeepFilters: Boolean =
    getOptBooleanPropOrConf(ConfKeys.InputDatastatsKeepFilters).getOrElse(true)

  lazy val useShortDatasourceNames: Boolean = getOptBooleanPropOrConf(ConfKeys.UseShortDatasourceName).getOrElse(false)

  lazy val datasourceShortNameStrategy: DatasourceNamingStrategy =
    getOptStringConf(ConfKeys.DatasourceShortNameStrategy)
      .map(DatasourceNamingStrategy.withName)
      .getOrElse(DatasourceNamingStrategy.LastFolderAndFile)

  val logicalDatasourceNameStrategy: Option[DatasourceNamingStrategy] =
    getOptStringConf(ConfKeys.LogicalDatasourceNameStrategy)
      .map(DatasourceNamingStrategy.withName)

  val columnLineageFallbackStrategy: Option[ColumnNameMatcherStrategy] =
    getOptStringConf(ColumnLineageFallbackStrategy)
      .flatMap(LineageFallbackStrategy.withName)

  // rules used to decide what naming strategy to use based on DS path
  lazy val logicalDsNamePathBasedRules: Seq[PathBasedNamingRule] =
    getOptStringSeqConf(ConfKeys.LogicalDsNamePathBasedRules).toSeq.flatMap(PathBasedNamingRule.parseRuleList)

  lazy val shortDsNamePathBasedRules: Seq[PathBasedNamingRule]   =
    getOptStringSeqConf(ConfKeys.DsShortNamePathBasedRules).toSeq.flatMap(PathBasedNamingRule.parseRuleList)

  def getDatasourceLogicalName(path: String): Seq[String] = {
    val ldsName: Option[String] = DatasourceLogicalNameOverrides
      .maybeOverridenValue(path)
      .orElse(
        logicalDatasourceNameStrategy
          .flatMap(st => DatasourceNamingStrategy.convert(st, path, logicalDsNamePathBasedRules))
      )
      .map("logical::" + _)
    val ldsLocation             = Seq(path).map("logicalLocation::" + _) // FIXME: we want some rules to customize this
    ldsName.toSeq ++ ldsLocation
  }

  def getProjectsFromConfig(
    apiWrapper: BatchDamEntitiesApiWrapper,
    defaultProject: Option[String]
  ): Option[List[ProjectRef]] = {
    import apiWrapper.Implicits._
    getOptStringSeqConf(ConfKeys.Projects)
      .orElse(Some(defaultProject.toList))
      .map { projects =>
        projects.map(projectName => Project(pk = ProjectPK(name = projectName)).enqueToBatch().toRef).toList
      }.filter(ps => ps.nonEmpty) // return None in case of empty projects list
  }

  def explicitProcessRunName: Option[String] = getOptStrPropOrConfOrEnvVar(ConfKeys.ExplicitProcessRunName)

  def await[T](fn: Future[T]) =
    Await.result(fn, duration.Duration.Inf) // FIXME

//  lazy val statsConfs = VariousStatsConfs(
//    inputDs = readStatsConf(
//      dsType = ConfKeys.statsDsInput,
//      default = StatsConf(computeQuantiles = false, cachePerPath = true, coalesceEnabled = true, coalesceWorkers = 1)
//    ),
//    // output stats more dangerous to coalesce, as it'll definitely may increase OOM or disk overfilling likelyhood
//    outputDs = readStatsConf(
//      dsType = ConfKeys.statsDsOutput,
//      default =
//        StatsConf(computeQuantiles = false, cachePerPath = false, coalesceEnabled = false, coalesceWorkers = 100)
//    )
//  )
//
//  def readStatsConf(
//                     dsType: String,
//                     default: StatsConf
//                   ) = {
//    def getBoolStatConf(confName: String) = getOptBooleanPropOrConf(ConfKeys.getStatsConfKey(dsType = dsType, confName))
//    def getIntStatConf(confName: String) =
//      getOptLongPropOrConf(ConfKeys.getStatsConfKey(dsType = dsType, confName)).map(_.toInt)
//    StatsConf(
//      computeQuantiles = getBoolStatConf(ConfKeys.computeQuantiles).getOrElse(default.computeQuantiles),
//      cachePerPath = getBoolStatConf(ConfKeys.cachePerPath).getOrElse(default.cachePerPath),
//      coalesceEnabled = getBoolStatConf(ConfKeys.coalesceEnabled).getOrElse(default.coalesceEnabled),
//      coalesceWorkers = getIntStatConf(ConfKeys.coalesceWorkers).getOrElse(default.coalesceWorkers)
//    )
//  }

}

final case class DefaultSparkEnvironnementProvider(
  val configuration: ReadableConfig,
  val apiWrapper: BatchDamEntitiesApiWrapper
) extends EnvironnementProvider
    with FlinkConfExtractor {
  import apiWrapper.Implicits._

  val getUser: Option[User]               =
    getOptStringConf(ConfKeys.User).map(name => User(UserPK(name)).enqueToBatch())

  val getCodeVersion: Option[CodeVersion] = for {
    codeRepo    <- getOptStringConf(ConfKeys.CodeRepo).map(repo =>
                     CodeBase(CodeBasePK(repo)).enqueToBatch()
                   )
    maintainers <- getOptStringSeqConf(ConfKeys.CodeMaintainers).map(maintainers =>
                     maintainers.toList.map(m => User(UserPK(m)).enqueToBatch().toRef)
                   )
    codeVersion <- getOptStringConf(ConfKeys.CodeVersion).map(version =>
                     CodeVersion(CodeVersionPK(version, codeRepo), maintainers).enqueToBatch()
                   )
  } yield codeVersion

  def explicitProcessName: Option[String] = getOptStrPropOrConfOrEnvVar(ConfKeys.ExplicitProcessName)

  val getRuntimeEnvironment: Option[String] = getOptStringConf(ConfKeys.RuntimeEnvironment)

  def getProjects(defaultProject: Option[String]): Option[List[ProjectRef]] =
    getProjectsFromConfig(apiWrapper, defaultProject)
}
