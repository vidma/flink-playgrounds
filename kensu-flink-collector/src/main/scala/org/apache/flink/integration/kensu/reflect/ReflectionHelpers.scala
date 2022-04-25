package org.apache.flink.integration.kensu.reflect

import org.apache.flink.integration.kensu.CollectorRecoverableFailure

object ReflectionHelpers {

  def maybeReflGet[T](obj: Object, fieldName: String): Option[T] =
    // FIXME: handle type cast errors
    Option(new ReflectHelpers[T]().reflectGetField(obj, fieldName))

  def reflOrThrow[T](obj: Object, fieldName: String): T =
    maybeReflGet[T](obj, fieldName)
      .getOrElse(throw CollectorRecoverableFailure(
        s"unable to access field  '$fieldName' or field does not exist on object: ${obj}"
      ))
}
