package io.kensu.lineage.utils

import org.slf4j.{Logger, LoggerFactory}

trait DamLogging {
  // very very often log4j gives conflicts (e.g. in tests), so include alternative poor man's logging for now
  val LOGGING_DEBUG = false

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def damLogWarn(msg: => String, logStdout: Boolean = false) = {
    if (LOGGING_DEBUG || logStdout) println(s"Kensu: $msg")
    LOG.warn(s"Kensu: $msg")
  }

  def damLogInfo(msg: => String) = {
    if (LOGGING_DEBUG) println(msg)
    LOG.warn(s"Kensu: $msg") // FIXME - change to info
  }

  def damLogError(msg: => String, cause: Throwable) = {
    if (LOGGING_DEBUG) {
      println(msg)
      cause.printStackTrace()
    }
    LOG.warn(s"Kensu: $msg", cause)
  }

}
