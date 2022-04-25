package org.apache.flink.integration.kensu.sinks

object KensuDynamicTableSink {
  // based on https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sourcessinks/#dynamic-table-sink

}

import io.kensu.dam.lineage.spark.lineage.EnvironnementProvider
import io.kensu.dam.model.ModelHelpers.{DataStatsHelper, LineageRunHelper}
import io.kensu.dam.model._
import org.apache.flink.configuration.{ConfigOption, ReadableConfig}
import org.apache.flink.integration.kensu.KensuFlinkHook.{logDebug, logInfo, logVarWithType}
import org.apache.flink.integration.kensu.KensuStatsHelpers.convertStatToKensu
import org.apache.flink.integration.kensu.StatConstants._
import org.apache.flink.integration.kensu.StatsInputsExtractor.nullSafeAny
import org.apache.flink.integration.kensu.sinks.IngestionHelpers.{lineageRefById, processRunRefById, schemaRefFromId}
import org.apache.flink.integration.kensu.sinks.ingestion.DAMClientFactory
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object KensuStatsDynamicTableSink { // define all options statically
//  val HOSTNAME: ConfigOption[String] = ConfigOptions.key("hostname").stringType.noDefaultValue
//  val PORT: ConfigOption[Integer] = ConfigOptions.key("port").intType.noDefaultValue
//  val BYTE_DELIMITER: ConfigOption[Integer] = ConfigOptions.key("byte-delimiter").intType.defaultValue(10) // corresponds to '\n'
}

class KensuStatsDynamicTableSink extends DynamicTableSinkFactory {
  override def factoryIdentifier = "kensu-stats-reporter" // used for matching to `connector = '...'`

  override def requiredOptions: util.Set[ConfigOption[_]] = {
    val options = new util.HashSet[ConfigOption[_]]()
    // options.add(FactoryUtil.FORMAT) // use pre-defined option for format
    options
  }

//  override def optionalOptions: Nothing = {
//    val options = new Nothing
//    options.add(SocketDynamicTableFactory.BYTE_DELIMITER)
//    options
//  }

  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = { // either implement your custom validation logic here ...
    // or use the provided helper utility
    // val helper = FactoryUtil.createTableFactoryHelper(this, context)
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
    // val producedDataType = context.getCatalogTable.getResolvedSchema.toPhysicalRowDataType
    // create and return dynamic table source
//    new Nothing(hostname, port, byteDelimiter, decodingFormat, producedDataType)
    val dataType: DataType        = context.getCatalogTable.getResolvedSchema.toPhysicalRowDataType
    // FIXME: this conf is only from `CREATE TABLE ... WITH ( ... )`, how do we access the global flink conf?
    //  FIXME: no better way than forwarding conf via `CREATE TABLE`?
    val flinkConf: ReadableConfig = context.getConfiguration
    val tableName                 = context.getObjectIdentifier.asSummaryString()
    new KensuSink(dataType, flinkConf, tableName)
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

object IngestionHelpers {
  def schemaRefFromId(id: String)   = SchemaRef(byGUID = Some(id), byPK = None)
  def processRunRefById(id: String) = ProcessRunRef(byGUID = Some(id), byPK = None)
  def lineageRefById(id: String)    = ProcessLineageRef(byGUID = Some(id), byPK = None)
}

class KensuSink(
  dataType: DataType,
  flinkConf: ReadableConfig,
  _sinkTableName: String
) extends DynamicTableSink {

  override def getChangelogMode(changelogMode: ChangelogMode): ChangelogMode = ChangelogMode.all()

  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider =
    new SinkFunctionProvider {
      // logInfo(s"[getSinkRuntimeProvider] initializing ${this.getClass.getName} for sinkTable=${_sinkTableName}")

      // FIXME: RichSinkFunction could handle open/close
      override def createSinkFunction(): SinkFunction[RowData] = new SinkFunction[RowData] {
        private final val rowDataType    = dataType
        private final val innerFlinkConf = flinkConf
        private final val sinkTableName  = _sinkTableName

        // akka serialization issues?
        @transient lazy val kensuClient = DAMClientFactory.getDAMClient(innerFlinkConf)

        // logInfo(s"[createSinkFunction] initializing ${this.getClass.getName} for sinkTable=${_sinkTableName}")

        override def invoke(rowData: RowData, context: SinkFunction.Context): Unit =
          this.rowDataType.getLogicalType match {
            case r: RowType =>
              logInfo(s"stats computed. start extracting stats in ${sinkTableName}")
              @transient lazy val kensuApi                    = kensuClient.toBatchContainer
              @transient implicit lazy val envProvider        = EnvironnementProvider.create(innerFlinkConf, kensuApi)
              import kensuApi.Implicits._
              val fieldsByName: Map[String, RowType.RowField] =
                r.getFields.asScala.groupBy(_.getName).mapValues(_.head).toMap
              logInfo(fieldsByName.toString())

              def getField(fieldName: String) = {
                val result = fieldsByName.get(fieldName).flatMap { f =>
                  val fieldGetter = RowData.createFieldGetter(f.getType, r.getFieldIndex(f.getName))
                  val fieldValue  = fieldGetter.getFieldOrNull(rowData)
                  logVarWithType(s"getField('$fieldName'): orig value=", fieldValue)
                  Option(fieldValue)
                }
                result
              }

              def getStrField(fieldName: String): Option[String] =
                getField(fieldName).flatMap {
                  // class matching seem to fail... weird...
                  case str if str.getClass.toString == "class org.apache.flink.table.data.binary.BinaryStringData" =>
                    Some(str.toString)
                  // case str: String => Some(str)
                  case otherValue                                                                                  =>
                    logVarWithType(s"getStrField('$fieldName'): got unexpected value=", otherValue)
                    None
                }

              def getTimestampFieldMillis(fieldName: String) =
                getField(fieldName).flatMap {
                  case t: TimestampData => Some(t).map(_.getMillisecond)
                  case otherValue       =>
                    logVarWithType(s"getTimestampFieldMillis('$fieldName'): got unexpected value=", otherValue)
                    None
                }

              logInfo(s"getStrField(Ds_Path_Alias): ${getStrField(Ds_Path_Alias)}")
              for {
                dsPath       <- getStrField(Ds_Path_Alias)
                _             = logInfo(s"computed stats for DS path=$dsPath")
                schemaId     <- getStrField(Ds_SchemaId_Alias)
                processRunId <- getStrField(Ds_ProcessRunId_Alias)
                lineageId    <- getStrField(Ds_LineageId_Alias)
                // P.S. using current timestamp instead of window group timestamp, and the later can be much in the past
                // and elsewhere DataStats timestamps are based on time of processing
                datastatsTs   = System.currentTimeMillis() // getTimestampFieldMillis(Stats_Window_End_Alias)
              } yield {
                logInfo(
                  s"start extracting stats for dsPath=$dsPath , schemaId=$schemaId, processRunId=$processRunId , lineageId=$lineageId , ts=$datastatsTs"
                )

                var stats = Map.empty[String, Double]

                // FIXME: how to access flink conf from here?
                r.getFields.asScala.foreach { f =>
                  val fieldGetter = RowData.createFieldGetter(f.getType, r.getFieldIndex(f.getName))
                  val fieldValue  = fieldGetter.getFieldOrNull(rowData)
                  logInfo(
                    s"field ${f.getName}='${nullSafeAny(fieldValue)}' [${f.getType}]"
                  )

                  if (!f.getName.startsWith(ksuDataPrefix)) {
                    val (kensuStatName, maybeStatValue) =
                      convertStatToKensu(fieldName = f.getName, fieldValue = fieldValue)
                    logInfo(
                      s"Extracted kensu stat ${kensuStatName}:'${maybeStatValue}' [${f.getType}]"
                    )
                    maybeStatValue.foreach { statValue =>
                      stats += kensuStatName -> statValue
                    }
                  }
                }

                if (stats.nonEmpty) {
                  val statsLinRun = LineageRun(LineageRunPK(
                    processRunRef = processRunRefById(processRunId),
                    lineageRef    = lineageRefById(lineageId),
                    timestamp     = datastatsTs
                  )).enqueToBatch()

                  val statsEntity = DataStats(
                    pk          = DataStatsPK(
                      schemaRef     = schemaRefFromId(schemaId),
                      /* Points to the lineage run which reports the stats data (different than lineage was reported in) */
                      lineageRunRef = statsLinRun.toRef
                    ),
                    stats       = stats,
                    extraAsJson = None
                  ).enqueToBatch()
                  logDebug("statsEntity=" + statsEntity)
                  logInfo(s"sending out stats to Kensu...")
                  val future      = kensuApi.reportToDamAndLogErrorNonBlocking(thowErr = false)
                  Await.result(future, 30.seconds)
                  logInfo(s"sent stats to Kensu...")
                }
              }
            case x          =>
              logVarWithType(s"KensuSink got unexpected ROW datatype", x)
          }
//      override def PrintSinkFunction(sinkIdentifier: String, stdErr: Boolean) {
//        this.writer = new PrintSinkOutputWriter[_](sinkIdentifier, stdErr)
//      }
      }
    }

  override def copy(): DynamicTableSink = new KensuSink(dataType, flinkConf, _sinkTableName)

  override def asSummaryString(): String = this.toString
}
