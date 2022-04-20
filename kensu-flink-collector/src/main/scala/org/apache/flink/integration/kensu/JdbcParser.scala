package org.apache.flink.integration.kensu

import org.apache.flink.api.java.typeutils.runtime.RowSerializer
import org.apache.flink.connector.jdbc.JdbcConnectionOptions
import org.apache.flink.connector.jdbc.internal.connection.{JdbcConnectionProvider, SimpleJdbcConnectionProvider}
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat
// flink: 1.13.x: AbstractJdbcOutputFormat, JdbcBatchingOutputFormat,
import org.apache.flink.integration.kensu.JdbcEntities.extractSinkFunction
import org.apache.flink.integration.kensu.KensuFlinkHook.{logInfo, logVarWithType}
import org.apache.flink.integration.kensu.reflect.ReflectionHelpers.maybeReflGet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.graph.StreamNode
import org.apache.flink.table.types.logical.LogicalType

import java.util.List
import scala.util.matching.Regex

case class CollectorRecoverableFailure(msg: String) extends RuntimeException(msg)

final case class JdbcConnectionInfo(
  // jdbc:mysql://mysql:3306/sql-demo
  databaseUrl: String,
  // com.mysql.jdbc.Driver
  driverName: String
) {
  // FIXME: explicit port or not!
  lazy val kensuDatabaseUri = databaseUrl.stripPrefix("jdbc:")
  override def toString     = kensuDatabaseUri
}

object JdbcParser {

  def addSinkEntity(sinkFunction: SinkFunction[_], sinkNode: StreamNode, ret: List[String]): Unit = {
    println("SCAAALA")
    logVarWithType("SINK FROM SCALA:", sinkNode)

    val jdbcSink                          = extractSinkFunction(sinkFunction)
    // basically this do just this, over reflection
    // jdbcSink.outputFormat.connectionProvider.jdbcOptions
    // JdbcRowOutputFormat
    val dbUri: Option[JdbcConnectionInfo] = maybeReflGet[JdbcOutputFormat[_, _, _]](jdbcSink, "outputFormat").collect {
      // here we're matching on a particular datatype, there might be more
      case outputFormat: JdbcOutputFormat[_, _, _] =>
        maybeReflGet[SimpleJdbcConnectionProvider](outputFormat, "connectionProvider").flatMap { jConnProv =>
          maybeReflGet[JdbcConnectionOptions](jConnProv, "jdbcOptions").map { jdbcOptions =>
            JdbcConnectionInfo(
              databaseUrl = jdbcOptions.getDbURL,
              driverName  = jdbcOptions.getDriverName
            )
          }
        }

      case x =>
        logVarWithType("Found unsupported JDBC outputFormat:", x)
        None
    }.flatten
    logVarWithType("JDBC DB URL:", dbUri)

    println("getTypeSerializerOut")
    import scala.collection.JavaConverters._
    println(sinkNode.getTypeSerializersIn.toSeq)
    // FIXME: in flink 1.13.x it was: val types: Seq[LogicalType] = sinkNode.getTypeSerializersIn.toSeq.collect { case x: org.apache.flink.table.runtime.typeutils.RowDataSerializer =>
    val types: Seq[LogicalType] = sinkNode.getTypeSerializersIn.toSeq.collect {
      case x: RowSerializer =>
        // here are types, but don't know columns:
        // jobmanager_1      | org.apache.flink.integration.kensu - logInfo - RowDataSerializer.one of  input types:BIGINT[class=class org.apache.flink.table.types.logical.BigIntType]
        // jobmanager_1      | org.apache.flink.integration.kensu - logInfo - RowDataSerializer.one of  input types:TIMESTAMP(3)[class=class org.apache.flink.table.types.logical.TimestampType]
        // jobmanager_1      | org.apache.flink.integration.kensu - logInfo - RowDataSerializer.one of  input types:BIGINT[class=class org.apache.flink.table.types.logical.BigIntType]
        // columns are only in opName, which maybe might be shortener?
        // FIXME: this has changed in RowSerializer..
        val types: Seq[LogicalType] = maybeReflGet[Array[LogicalType]](x, "types")
          .toSeq.flatMap(_.toSeq).map { t =>
            logVarWithType("RowDataSerializer.one of  input types:", t)
            t
          }
        types
    }.flatten
    val fieldNames: Seq[String] = extractFieldNames(sinkNode)
    println(s"fieldNames: ${fieldNames}")
    println(s"fieldTYpes: ${types}")
    val fieldsWithTypes         = (fieldNames zip types.map(_.toString)).toMap
    println(s"fieldsWithTypes: ${fieldsWithTypes}")

//    for {
//      loc <- dbUri
//    } yield DataSource(
//      location = loc.kensuDatabaseUri,
//
//    )
  }

  // FIXME: we need a better impl here, maybe with bytecode modification with ASM5?
  def extractFieldNames(sinkNode: StreamNode): Seq[String] = {
    val opName = sinkNode.getOperatorName
    // Sink(table=[default_catalog.default_database.spend_report], fields=[account_id, log_ts, EXPR$0])
    // drop(1)
    "Sink\\(table=\\[(.+)\\], fields=\\[([^]]+,? ?)+\\]".r.findFirstMatchIn(opName).map(
      _.subgroups.drop(1)
    ).toSeq.flatten.flatMap(_.split(",")).map(_.trim)
    // result: List(account_id, log_ts, EXPR$0)
  }

  def getTableName(sinkNode: StreamNode) = {
    val opName = sinkNode.getOperatorName
    // Sink(table=[default_catalog.default_database.spend_report], fields=[account_id, log_ts, EXPR$0])
    "Sink\\(table=\\[(.+)\\],".r.findFirstMatchIn(opName).map(_.group(1))
    // val res15: Option[String] = Some(default_catalog.default_database.spend_report)
  }
}
