package org.apache.flink.integration.kensu.sinks.ingestion

import java.io.File
import java.util.concurrent.atomic.AtomicReference
import akka.actor.{ActorSystem, Terminated}
import com.typesafe.config.{Config, ConfigFactory}
import io.swagger.client.core.{ApiInvoker, OfflineApiInvoker, RawHttpBackendHeader}

import scala.concurrent.{Await, ExecutionContext, Future}
import io.kensu.dam._
import io.kensu.dam.lineage.spark.lineage.{BatchDamEntitiesApiWrapper, DamEntitiesApiWrapper}
import io.kensu.lineage.utils.{ConfKeys, DamLogging, DefaultBlockingIOExecutionContext, FlinkConfExtractor, TimeUtils}

import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicReference
import org.apache.flink.configuration.{Configuration, ReadableConfig}

import scala.util.Try
import scala.util.control.NonFatal

/**
 * The object contains static information about settings needed for initialization of the DAMDataLineageWriter class.
 */
object DAMClientFactory {

  def getDAMClient(configuration: ReadableConfig): DamEntitiesApiWrapper = {
    val cfg                       = new DamClientConfigurator(configuration)
    implicit val actorSystem      = DamClientActorSystem.getOrCreateActorSystem()
    implicit val executionContext =
      if (cfg.useIoExecContext) new DefaultBlockingIOExecutionContext(actorSystem)
      else ExecutionContext.Implicits.global

    implicit val apiInvoker: ApiInvoker =
      if (cfg.isOffline) {
        val f         = new File(cfg.damOfflineFileName)
        val outStream = new java.io.FileOutputStream(f, true)
        DamClientActorSystem.registerFlushOutputFn {
          outStream.flush()
        }
        DamClientActorSystem.registerShutdownCleanupFn {
          outStream.close()
        }
        new OfflineApiInvoker(out = outStream)
      } else {
        createApiInvoker(cfg)
      }
    val api                             = new ManageKensuDAMEntitiesApi(enableCompaction = cfg.enableEntityEntityCompaction)
    // Actor system needs to be destroyed on end of spark application otherwise the JVM would be blocked
//    scheduleActorShutdownOnSparkAppExist(
//      spark,
//      actorSysTimeout = 30.seconds,
//      futuresTimeout = cfg.damShutdownTimeoutSeconds.seconds
//    )
    DamEntitiesApiWrapper(
      apiInvoker       = apiInvoker,
      api              = api,
      executionContext = executionContext,
      actorSystem      = actorSystem
    )
  }

  private def createApiInvoker(cfg: DamClientConfigurator)(implicit actorSystem: ActorSystem) = {
    val userAgent     = getSparkCollectorUserAgent
    val forcedHeaders = List(
      RawHttpBackendHeader("User-Agent", userAgent)
    )
    ApiInvoker.createDefaultInvokerWithForcedHeaders(forcedHeaders, toDamClientConfig(cfg, userAgent))
  }

  private def toDamClientConfig(cfg: DamClientConfigurator, userAgent: String): Config = {
    // FIXME: is there a better way to transform apache commons config into typesafe config!?
    val cfgMap = Map(
      "dam.uri" -> cfg.serverHost,
      "dam.auth.token" -> cfg.authToken,
      "io.swagger.client.apiRequest.trust-certificates" -> cfg.trustSelfSignedCertificates,
      "io.swagger.client.apiRequest.default-headers.userAgent" -> userAgent
    )
    import scala.collection.JavaConverters._
    ConfigFactory
      .parseMap(cfgMap.asJava)
      .withFallback(ConfigFactory.load())
  }

  private def getSparkCollectorUserAgent: String = "TODO" // TODO

//  def scheduleActorShutdownOnSparkAppExist(
//                                            s: SparkSession,
//                                            actorSysTimeout: Duration = 30.second,
//                                            futuresTimeout: Duration = 30.second
//                                          ): Unit =
//    s.sparkContext.addSparkListener(new SparkListener {
//      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
//      // FIXME: doesn't seem to help without spark.stop...
//        DamClientActorSystem.destroyActorSystem(actorSysTimeout = actorSysTimeout, futuresTimeout = futuresTimeout)
//    })
}

object DamClientActorSystem extends DamLogging {

  private lazy val actorSystem: ActorSystem = {
    logInfo("Creating a new DAM scala client actor-system")
    val useShadedAkka = Option(System.getenv("KSU_USE_SHADED_ACTOR_SYSTEM"))
      .map(_ == "true")
      .getOrElse(false) // FIXME?
    val sys           =
      if (useShadedAkka) {
        ActorSystem("dam-scala-client", ConfigFactory.load("ksu_shaded_aka_actor"))
      } else {
        // non shaded Actor system is currently used only in unit tests
        ActorSystem("dam-scala-client")
      }
    logInfo("Done creating a new DAM scala client actor-system.")
    sys
  }

  private val registeredFuturesRef      = new AtomicReference[Seq[Future[Any]]](Seq.empty)
  private val registeredCleanupFns      = new AtomicReference[Seq[() => Unit]](Seq.empty)
  private val registeredFlushOutputsFns = new AtomicReference[Seq[() => Unit]](Seq.empty)

  private def logInfo(msg: String) = {
    // FIXME: think about logback default config (we have no control of it and don't want to interfer with customer's one)
    println(msg)
    damLogInfo(msg)
  }

  def getOrCreateActorSystem() = synchronized {
    actorSystem
  }

  def registerFlushOutputFn(fn: => Unit): Unit = {
    import io.kensu.dam.utils.AtomicRefUtils.AtomicRefUpdateHelper
    registeredFlushOutputsFns.atomicallyUpdate(registeredFns => registeredFns :+ (fn _))
  }

  def registerShutdownCleanupFn(fn: => Unit): Unit = {
    import io.kensu.dam.utils.AtomicRefUtils.AtomicRefUpdateHelper
    registeredCleanupFns.atomicallyUpdate(registeredFns => registeredFns :+ (fn _))
  }

  def registerFutureToAwait(f: Future[Any]): Unit = {
    import io.kensu.dam.utils.AtomicRefUtils.AtomicRefUpdateHelper
    registeredFuturesRef.atomicallyUpdate(registeredFutures => registeredFutures :+ f)
  }

  private def uncompletedFuturesExist() =
    registeredFuturesRef.get().exists(!_.isCompleted)

  private def awaitFuturesIteration(timeout: Duration)(implicit ec: ExecutionContext): Unit = {
    val futures           = registeredFuturesRef.get()
    val notFailingFutures = futures.map(_.recover { case NonFatal(_) => () })
    val combined          = Future.sequence(notFailingFutures)
    Await.result(combined, atMost = timeout)
  }

  def awaitDamReportingToFinish(timeout: Duration)(implicit ec: ExecutionContext): Unit = {
    logInfo("Awaiting for DAM events reporting to finish")
    var timeRemaining = timeout
    while ((timeRemaining > Duration.Zero) && uncompletedFuturesExist()) {
      logInfo("Starting a new iteration of waiting for DAM reporting to finish...")
      val alreadyAwaited = TimeUtils.measureDuration(awaitFuturesIteration(timeRemaining))
      // once (first) futures are finished, they may have created/registered new futures say for datastats or reporting,
      // so we need to repeat the waiting process once more if time limit allows
      timeRemaining = (timeRemaining - alreadyAwaited).max(Duration.Zero)
    }
    logInfo("DAM events reporting finished")

    logInfo("Flushing outputs if any")
    registeredFlushOutputsFns.get().foreach(fn => Try(fn()))
    logInfo("Flushing outputs done (if any)")
  }

  def awaitRegisteredFutures(timeout: Duration)(implicit ec: ExecutionContext): Unit = {
    awaitDamReportingToFinish(timeout)
    logInfo("Closing resources")
    registeredCleanupFns.get().foreach(fn => Try(fn()))
    logInfo("Closing resources done")
  }

  def destroyActorSystem(actorSysTimeout: Duration = 30.second, futuresTimeout: Duration = 30.second): Unit =
    synchronized {
      import ExecutionContext.Implicits.global
      logInfo("Start terminating DAM events reporting actor-system...")
      awaitRegisteredFutures(futuresTimeout) // FIXME: configurable timeout
      def terminateActorSystem =
        actorSystem.terminate().map(_ => logInfo("DAM scala client actor-system terminated"))
      Await.result(terminateActorSystem, atMost = actorSysTimeout)
    }
}

class DamClientConfigurator(val configuration: ReadableConfig) extends FlinkConfExtractor {

  val useIoExecContext = getOptStringConf(ConfKeys.SenderExecutionContext)
    .forall(_ == "io-execution-context")

  val isOffline = sys.env
    .get("DAM_OFFLINE")
    .map(_.toLowerCase == "true")
    .orElse(getOptBooleanConf(ConfKeys.IsOffline))
    .getOrElse(false)

  lazy val damOfflineFileName: String = sys.env
    .get("DAM_OFFLINE_FILENAME")
    .orElse(getOptStringConf(ConfKeys.OfflineFileName))
    .getOrElse("dam-offline.log")

  lazy val damShutdownTimeoutSeconds: Long = sys.env
    .get("DAM_SHUTDOWN_TIMEOUT")
    .flatMap(s => Try(s.toLong).toOption)
    .orElse(getOptLongConf(ConfKeys.DamShutdownTimeout))
    .getOrElse(60 * 5)

  val trustSelfSignedCertificates: Boolean = sys.env
    .get("DAM_IGNORE_SSL")
    .map(_.toLowerCase == "true")
    .orElse(getOptBooleanConf(ConfKeys.IgnoreSslCert))
    .getOrElse(false)

  val enableEntityEntityCompaction: Boolean = sys.env
    .get("DAM_ENTITY_COMPACTION")
    .map(_.toLowerCase == "true")
    .orElse(getOptBooleanConf(ConfKeys.EnableEntityCompaction))
    .getOrElse(true)

  lazy val authToken: String = sys.env
    .get("AUTH_TOKEN")
    .orElse(sys.props.get(ConfKeys.AuthToken))
    .orElse(getOptStringConf(ConfKeys.AuthToken))
    .getOrElse(
      throw new IllegalArgumentException(
        s"Missing environment variable `AUTH_TOKEN`, or ${ConfKeys.AuthToken} in configuration ${configuration}"
      )
    )

  lazy val serverHost: String = sys.env
    .get("DAM_INGESTION_URL")
    .orElse(getOptStringConf(ConfKeys.ServerHost))
    .orElse(getOptStringConf("dam.uri"))
    .getOrElse(throw new IllegalArgumentException(s"Missing configuration keys: ${ConfKeys.ServerHost} or dam.uri"))
}
