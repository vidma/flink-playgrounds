package org.apache.flink.integration.kensu.sinks

object KensuDynamicTableSink {
  // based on https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sourcessinks/#dynamic-table-sink

}

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.types.DataType

import java.util
import java.util.Set


object KensuStatsDynamicTableSink { // define all options statically
//  val HOSTNAME: ConfigOption[String] = ConfigOptions.key("hostname").stringType.noDefaultValue
//  val PORT: ConfigOption[Integer] = ConfigOptions.key("port").intType.noDefaultValue
//  val BYTE_DELIMITER: ConfigOption[Integer] = ConfigOptions.key("byte-delimiter").intType.defaultValue(10) // corresponds to '\n'
}

class KensuStatsDynamicTableSink extends DynamicTableSinkFactory {
  override def factoryIdentifier = "kensuStatsSink" // used for matching to `connector = '...'`

  override def requiredOptions: util.Set[ConfigOption[_]] = {
    val options = new util.HashSet[ConfigOption[_]]()
    //options.add(FactoryUtil.FORMAT) // use pre-defined option for format
    options
  }

//  override def optionalOptions: Nothing = {
//    val options = new Nothing
//    options.add(SocketDynamicTableFactory.BYTE_DELIMITER)
//    options
//  }

  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = { // either implement your custom validation logic here ...
    // or use the provided helper utility
    //val helper = FactoryUtil.createTableFactoryHelper(this, context)
    // discover a suitable decoding format
//    val decodingFormat = helper.discoverDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)
//    // validate all options
//    helper.validate()
//    // get the validated options
//    val options = helper.getOptions
//    val hostname = options.get(SocketDynamicTableFactory.HOSTNAME)
//    val port = options.get(SocketDynamicTableFactory.PORT)
//    val byteDelimiter = options.get(SocketDynamicTableFactory.BYTE_DELIMITER).asInstanceOf[Int].toByte
    // derive the produced data type (excluding computed columns) from the catalog table
    //val producedDataType = context.getCatalogTable.getResolvedSchema.toPhysicalRowDataType
    // create and return dynamic table source
//    new Nothing(hostname, port, byteDelimiter, decodingFormat, producedDataType)
    new KensuSink()
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = new util.HashSet[ConfigOption[_]]()
}

// https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sourcessinks/#dynamic-table-sink
// For regular batch scenarios, the sink can solely accept insert-only rows and write out bounded streams.
// For regular streaming scenarios, the sink can solely accept insert-only rows and can write out unbounded streams.
// For change data capture (CDC) scenarios, the sink can write out bounded or unbounded streams with insert, update, and delete rows.

// The runtime implementation of a DynamicTableSink must consume internal data structures.
// Thus, records must be accepted as org.apache.flink.table.data.RowData.
// The framework provides runtime converters such that a sink can still work on common data
// structures and perform a conversion at the beginning.


class KensuSink extends DynamicTableSink {
  override def getChangelogMode(changelogMode: ChangelogMode): ChangelogMode = ChangelogMode.all()

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = new SinkFunctionProvider {
    // FIXME
    val sinkIdentifier = "KensuSink"
    override def createSinkFunction(): SinkFunction[RowData] = new PrintSinkFunction(sinkIdentifier, false) {
    }
  }

  // FIXME
  override def copy(): DynamicTableSink = this

  override def asSummaryString(): String = this.toString
}
