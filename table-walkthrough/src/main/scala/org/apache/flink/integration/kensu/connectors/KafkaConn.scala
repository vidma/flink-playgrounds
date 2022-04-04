package org.apache.flink.integration.kensu.connectors

object KafkaConn {
  def toKensuDatasourceLocation(topic: String, kafkaUri: String): String = {
    // FIXME: do we need more processing due multiple formats?
    s"kafka://$kafkaUri/$topic"
  }
}
