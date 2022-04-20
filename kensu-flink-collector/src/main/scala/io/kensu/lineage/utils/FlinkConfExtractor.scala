package io.kensu.lineage.utils

import akka.japi.Option.scala2JavaOption
import org.apache.flink.configuration.description.Description
import org.apache.flink.configuration.{
  ConfigOption,
  ConfigOptionFactory,
  ConfigOptions,
  Configuration,
  FallbackKey,
  ReadableConfig
}

import java.util.NoSuchElementException
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.reflect.ClassTag

//import org.apache.commons.configuration.{ Configuration, ConversionException }
//import org.slf4s.Logging

import scala.util.Try
import scala.util.control.NonFatal

trait FlinkConfExtractor extends DamLogging {
  def configuration: ReadableConfig

  def maybeEnvVar(key: String): Option[String] = {
    val envVarName = key.toUpperCase.replace(".", "_")
    sys.env.get(envVarName)
  }

  def getOptBooleanPropOrConf(key: String): Option[Boolean] =
    sys.props
      .get(key)
      .map(_.toLowerCase == "true")
      .orElse(getOptBooleanConf(key))

  def getOptLongPropOrConf(key: String): Option[Long] =
    sys.props
      .get(key)
      .map(_.toLong)
      .orElse(getOptLongConf(key))

  def getOptStrPropOrConf(key: String): Option[String] =
    sys.props
      .get(key)
      .orElse(getOptStringConf(key))

  def getOptStrPropOrConfOrEnvVar(key: String): Option[String] =
    getOptStrPropOrConf(key)
      .orElse(maybeEnvVar(key))

  // ----

  protected def getOptStringSeqConf(key: String): Option[Seq[String]] =
    getOptStringConf(key).map(_.split(","))

  protected def getOptFlinkConf[JT](key: String, confOption: ConfigOption[JT]): Option[JT] =
    getOptConf(key, configuration.getOptional[JT](confOption).asScala).flatten

  protected def getOptBooleanConf(key: String): Option[Boolean] =
    getOptFlinkConf[java.lang.Boolean](key, ConfigOptions.key(key).booleanType().noDefaultValue()).map(Boolean2boolean)

  protected def getOptStringConf(key: String): Option[String] =
    getOptFlinkConf[String](key, ConfigOptions.key(key).stringType().noDefaultValue())

  protected def getOptLongConf(key: String): Option[Long] =
    getOptFlinkConf[java.lang.Long](key, ConfigOptions.key(key).longType().noDefaultValue()).map(Long2long)

  private def getOptConf[T](key: String, getConfFn: => T): Option[T] =
    Try(Option(getConfFn)).recover {
      case e: NoSuchElementException =>
        damLogInfo(s"Config option $key not found, using default")
        throw e
//      case e: ConversionException =>
//        damLogInfo(s"Likely wrong data type for config option $key", e)
//        throw e
      case NonFatal(e)               =>
        damLogInfo(s"Issue with config option $key" + e.toString)
        throw e
    }.toOption.flatten
}
