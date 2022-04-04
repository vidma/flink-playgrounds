package org.apache.flink.integration.kensu.sinks

object KensuDynamicTableSink {
  // based on https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sourcessinks/#dynamic-table-sink

}

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.integration.kensu.KensuFlinkHook.{logInfo, logVarWithType}
import org.apache.flink.integration.kensu.StatsInputsExtractor.{nullSafe, nullSafeAny}
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

import java.util
import java.util.Set
import scala.collection.JavaConverters.asScalaBufferConverter


object KensuStatsDynamicTableSink { // define all options statically
//  val HOSTNAME: ConfigOption[String] = ConfigOptions.key("hostname").stringType.noDefaultValue
//  val PORT: ConfigOption[Integer] = ConfigOptions.key("port").intType.noDefaultValue
//  val BYTE_DELIMITER: ConfigOption[Integer] = ConfigOptions.key("byte-delimiter").intType.defaultValue(10) // corresponds to '\n'
}

class KensuStatsDynamicTableSink extends DynamicTableSinkFactory {
  override def factoryIdentifier = "kensu-stats-reporter" // used for matching to `connector = '...'`

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
    val dataType: DataType = context.getCatalogTable.getResolvedSchema.toPhysicalRowDataType
    new KensuSink(dataType)
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


class KensuSink(
                 dataType: DataType
               ) extends DynamicTableSink {

  override def getChangelogMode(changelogMode: ChangelogMode): ChangelogMode = ChangelogMode.all()

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = new SinkFunctionProvider {
    // FIXME
    val sinkIdentifier = "KensuSink"
    //context.data
    // FIXME: RichSinkFunction could handle open/close
    override def createSinkFunction(): SinkFunction[RowData] = new SinkFunction[RowData] {
      private final val rowDataType =  dataType

      override def invoke(rowData: RowData, context: SinkFunction.Context): Unit = {
        this.rowDataType.getLogicalType match {
          case r: RowType =>
            r.getFields.asScala.foreach { f =>
              val fieldType = f.getType
              val fieldGetter = RowData.createFieldGetter(f.getType, r.getFieldIndex(f.getName))
              val fieldValue = fieldGetter.getFieldOrNull(rowData)
              org.apache.flink.integration.kensu.KensuFlinkHook.logInfo(
                s"field ${f.getName}='${nullSafeAny(fieldValue)}' [${f.getType}]")
              val kensuStatName = f.getName.replace("_ksu_", ".")
              // FIXME: convert timestamp to int!!!??
              val kensuStatValue = fieldValue match {
                case t: TimestampData =>
                  Some(t.getMillisecond.toString)
                case t: java.lang.Long => Some(t.toString)
                case _ =>
                  // FIXME: log
                  None
              }
              org.apache.flink.integration.kensu.KensuFlinkHook.logInfo(
                s"Extracted kensu stat ${kensuStatName}:'${kensuStatValue}' [${f.getType}]")
              // taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field account_id_ksu_distinct_values='5' [BIGINT]
              //taskmanager_1     | 2022-04-04 14:08:36,011 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field account_id_ksu_distinct_values='5' [BIGINT]
              //taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field account_id_ksu_min='1' [BIGINT]
              //taskmanager_1     | 2022-04-04 14:08:36,011 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field account_id_ksu_min='1' [BIGINT]
              //taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field account_id_ksu_max='5' [BIGINT]
              //taskmanager_1     | 2022-04-04 14:08:36,012 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field account_id_ksu_max='5' [BIGINT]
              //taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field transaction_time_ksu_min='2000-01-04T11:56:13' [TIMESTAMP(3)]
              //taskmanager_1     | 2022-04-04 14:08:36,013 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field transaction_time_ksu_min='2000-01-04T11:56:13' [TIMESTAMP(3)]
              //taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field transaction_time_ksu_max='2000-01-04T23:59:06' [TIMESTAMP(3)]
              //taskmanager_1     | 2022-04-04 14:08:36,014 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field transaction_time_ksu_max='2000-01-04T23:59:06' [TIMESTAMP(3)]
              //taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field amount_ksu_min='5' [BIGINT]
              //taskmanager_1     | 2022-04-04 14:08:36,014 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field amount_ksu_min='5' [BIGINT]
              //taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field amount_ksu_max='995' [BIGINT]
              //taskmanager_1     | 2022-04-04 14:08:36,015 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field amount_ksu_max='995' [BIGINT]
              //taskmanager_1     | org.apache.flink.integration.kensu - logInfo - field _ksu_nrows='133' [BIGINT]
              //taskmanager_1     | 2022-04-04 14:08:36,015 WARN  org.apache.flink.integration.kensu.KensuFlinkHook            [] - field _ksu_nrows='133' [BIGINT]
              //client_1          | Job has been submitted with JobID 375489dc6ca05d2693f9daa306fe7b85
            }
          case x =>
            logVarWithType(s"KensuSink got unexpected ROW datatype", x)

        }
      }
//      override def PrintSinkFunction(sinkIdentifier: String, stdErr: Boolean) {
//        this.writer = new PrintSinkOutputWriter[_](sinkIdentifier, stdErr)
//      }
    }
  }

  override def copy(): DynamicTableSink = new KensuSink(dataType)

  override def asSummaryString(): String = this.toString
}
