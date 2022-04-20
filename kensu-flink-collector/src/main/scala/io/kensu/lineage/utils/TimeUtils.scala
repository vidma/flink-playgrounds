package io.kensu.lineage.utils

import java.util.concurrent.atomic.AtomicReference

import io.kensu.dam.utils.AtomicRefUtils.AtomicRefUpdateHelper

import scala.concurrent.duration.FiniteDuration

object TimeUtils extends DamLogging {
  val mockedTime            = new AtomicReference[Option[Long]](None)
  val lastMockedUniqueValue = new AtomicReference[Long](0)

  /**
   * Allows to mock time so we could make dam collector to generate more or less predictable entities
   * @param needsUniqueValue
   * @return
   */
  def getCurrentTimestamp(needsUniqueValue: Boolean = false) = {
    val maybeMockedTime = mockedTime.get()
    maybeMockedTime match {
      case None                  => System.currentTimeMillis()
      case Some(mockedTimestamp) =>
        if (needsUniqueValue) {
          lastMockedUniqueValue.atomicallyUpdate(_ + 1000)
        } else mockedTimestamp
    }
  }

  def setMockedTime(mockTimestamp: Long, forceOverride: Boolean = false): Unit =
    if (mockedTime.get().isDefined && !forceOverride) {
      damLogInfo(s"Fake DAM collector time was already set before, can not set time again...")
    } else {
      mockedTime.atomicallyUpdate(_ => Some(mockTimestamp))
      lastMockedUniqueValue.atomicallyUpdate(_ => mockTimestamp)
    }

  def getMockedOrProvidedTs(providedTs: Long, needsUniqueValue: Boolean = false) =
    mockedTime.get() match {
      case None    => providedTs
      case Some(_) => getCurrentTimestamp(needsUniqueValue)
    }

  def measureDuration(fn: => Unit): FiniteDuration = {
    import scala.concurrent.duration._
    val start = System.currentTimeMillis()
    fn
    val end   = System.currentTimeMillis()
    (end - start).millis
  }

}
