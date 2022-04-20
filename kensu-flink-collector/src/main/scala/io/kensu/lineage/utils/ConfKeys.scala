package io.kensu.lineage.utils

import java.util.UUID
import io.kensu
import org.apache.flink.configuration.{Configuration, ReadableConfig}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import scala.util.Try

// TODO => these configuration keys might be worth extracting as a conventional way to do this linkage to users and repos
object ConfKeys {
  val AuthToken                = "kensu.ingestion.auth.token"
  val ServerHost               = "kensu.ingestion.url"
  val IsOffline                = "kensu.ingestion.is_offline"
  val OfflineFileName          = "kensu.ingestion.offline.file"
  val IgnoreSslCert            = "kensu.ingestion.ignore.ssl.cert"
  val SenderExecutionContext   = "kensu.ingestion.execution.context"
  val EnableEntityCompaction   = "kensu.ingestion.entity.compaction"
  val MockedCollectorTimestamp = "kensu.ingestion.advanced.mocked_timestamp"

  val AutomaticallyGatherDatastats                  = "kensu.spark.data_stats.enabled"
  val GatherInputDatastats                          = "kensu.spark.data_stats.input.enabled"
  val InputDatastatsOnlyLineageCols                 = "kensu.spark.data_stats.input.only_used_in_lineage"

  // when only_used_in_lineage=true, if input columns of some DS are not seen in column lineage AT ALL
  // => if enabled, use all input DS columns as a fallback
  val InputDatastatsAllColumnsFallbackWhenNoLineage =
    "kensu.flink.data_stats.input_lineage_missing.only_used_in_lineage.all_columns_fallback"
  val InputDatastatsKeepFilters                     = "kensu.flink.data_stats.input.keep_filters"

  //
  val datastatsPrefix                                   = "kensu.spark.data_stats"
  val statsDsInput                                      = "input"
  val statsDsOutput                                     = "output"
  // e.g.:
  // dam.spark.data_stats.input.computeQuantiles
  // dam.spark.data_stats.input.cachePerPath
  // dam.spark.data_stats.input.coalesceEnabled
  // dam.spark.data_stats.input.coalesceWorkers
  // and the same for *.output.* ...
  def getStatsConfKey(dsType: String, confName: String) = s"${datastatsPrefix}.${dsType}.${confName}"
  val computeQuantiles                                  = "computeQuantiles"
  val cachePerPath                                      = "cachePerPath"
  val coalesceEnabled                                   = "coalesceEnabled"
  val coalesceWorkers                                   = "coalesceWorkers"

  val DamShutdownTimeout          = "kensu.spark.shutdown_timeout"
  val FileDebugLoggerLevel        = "kensu.spark.file_debug.level"
  val FileDebugLoggerFileName     = "kensu.spark.file_debug.file_name"
  val FileDebugLoggerCaptureSpark = "kensu.spark.file_debug.capture_spark_logs"

  val UseShortDatasourceName        = "kensu.datasources.short.name"
  val DatasourceShortNameStrategy   = "kensu.datasources.short.naming_strategy"
  val LogicalDatasourceNameStrategy = "kensu.logical.datasources.naming_strategy"
  val LogicalDsNamePathBasedRules   = "kensu.logical.datasources.path_rules.naming_strategy"
  val DsShortNamePathBasedRules     = "kensu.datasources.path_rules.short.naming_strategy"

  val ColumnLineageFallbackStrategy = "kensu.spark.lineage.column_lineage_fallback_strategy"

  val Organization = "kensu.activity.organization"

  val EnvironnementProviderClassName = "kensu.activity.spark.environnement.provider"

  val CodeMaintainers        = "kensu.activity.code.maintainers"
  val CodeRepo               = "kensu.activity.code.repository"
  val CodeVersion            = "kensu.activity.code.version"
  val User                   = "kensu.activity.user"
  val RuntimeEnvironment     = "kensu.activity.environment"
  val Projects               = "kensu.activity.projects"
  val ExplicitProcessName    = "kensu.activity.explicit.process.name"
  val ExplicitProcessRunName = "kensu.activity.explicit.process_run.name"
}

class KensuTrackerConfigurationProvider(override val configuration: ReadableConfig) extends FlinkConfExtractor {
  import ConfKeys._
  val damPropertiesFileName = "dam.properties"

//  val propFileConfOpt = scala.util.Try(new PropertiesConfiguration(damPropertiesFileName)).toOption
//  val systemConfOpt = Some(new SystemConfiguration)
//  val hadoopConfOpt = Some(new HadoopConfiguration(sparkSession.sparkContext.hadoopConfiguration))
//
//  val conf = new CompositeConfiguration(
//    Seq(
//      hadoopConfOpt,
//      systemConfOpt,
//      propFileConfOpt
//    ).flatten.asJava
//  )

//  def parseFileDebugConf(s: String): Option[org.apache.log4j.Level] = {
//    import org.apache.log4j.Level
//    import scala.util.Try
//    val allowed = Set("INFO", "DEBUG", "WARN")
//    if (!allowed.contains(s)) {
//      None
//    } else {
//      Try(Level.toLevel(s.toUpperCase, Level.INFO)).toOption
//    }
//  }
//
//  def maybeDamFileDebugLevel: Option[Level] =
//    sys.props
//      .get(ConfKeys.FileDebugLoggerLevel)
//      .orElse(getOptStringConf(ConfKeys.FileDebugLoggerLevel))
//      .flatMap(parseFileDebugConf)
//      .orElse(None)

  def damFileDebugFileName: String =
    sys.props
      .get(ConfKeys.FileDebugLoggerFileName)
      .orElse(getOptStringConf(ConfKeys.FileDebugLoggerFileName))
      .getOrElse("dam-collector-debug-file.log")

  def damFileDebugCaptureSparkLogs: Boolean =
    sys.props
      .get(ConfKeys.FileDebugLoggerCaptureSpark)
      .map(_.toLowerCase == "true")
      .orElse(getOptBooleanConf(ConfKeys.FileDebugLoggerCaptureSpark))
      .getOrElse(false)

  def damMockedCollectorTimestamp: Option[Long] =
    sys.props
      .get(ConfKeys.MockedCollectorTimestamp)
      .map(_.toLong)
      .orElse(getOptLongConf(ConfKeys.MockedCollectorTimestamp))

//  def setDamIngestionUrl(url: String) = updateConf(ServerHost, url)
//  def setAuthToken(token: String) = updateConf(AuthToken, token)
//
//  def setCurrentUser(user: String) = updateConf(User, user)
//  def setCurrentCode(repository: String, version: String) = {
//    updateConf(CodeRepo, repository)
//    updateConf(CodeVersion, version)
//  }

}

object CustomDatastatsSparkJobsEc {

  // the goal of this specific EC is to limit datastats impact on other Spark jobs,
  // this means we'll give one thread to Spark driver EC (not 100% sure yet if very good idea),
  // but this driver is still free to create jobs for datastats on whatever number of remote spark executor nodes
  // (limited by df.coalesce() in DataStatsExtractor*.scala)
  // ----
  // FIXME: we may want to make the pool size configurable - but also want to be sure it's not spawned multiple times
  // without being destroyed when needed if Kensu collector is initialized multiple times
  @transient lazy val datastatsSparkJobsEc: ExecutionContext = {
    ExecutionContext.fromExecutorService(
      // new akka.dispatch.forkjoin.ForkJoinPool(2)
      Executors.newFixedThreadPool(1)
    )
  }
}
