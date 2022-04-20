package io.kensu.lineage.utils

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher

import scala.concurrent.ExecutionContextExecutor

class DefaultBlockingIOExecutionContext(
  system: ActorSystem,
  dispacherName: String = "akka.actor.default-blocking-io-dispatcher"
) extends ExecutionContextExecutor {
  private val dispatcher: MessageDispatcher          = system.dispatchers.lookup(dispacherName)
  override def execute(command: Runnable): Unit      = dispatcher.execute(command)
  override def reportFailure(cause: Throwable): Unit = dispatcher.reportFailure(cause)
}
