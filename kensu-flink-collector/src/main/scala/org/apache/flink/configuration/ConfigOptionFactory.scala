package org.apache.flink.configuration

import org.apache.flink.configuration.description.Description

import scala.reflect.ClassTag

object ConfigOptionFactory {

  def getConfOption[T](key: String)(implicit clstag: ClassTag[T]): ConfigOption[T] =
    new ConfigOption[T](key, clstag.getClass, Description.builder().build(), null.asInstanceOf[T], false)
}
