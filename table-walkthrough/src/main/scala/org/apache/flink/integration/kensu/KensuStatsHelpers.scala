package org.apache.flink.integration.kensu

import org.apache.flink.integration.kensu.KensuFlinkHook.{logInfo, logVarWithType}
import org.apache.flink.integration.kensu.StatsInputsExtractor.extractTableInputs
import org.apache.flink.integration.kensu.connectors.KafkaConn
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.Expressions.{$, lit}
import org.apache.flink.table.api.bridge.java.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.internal.TableImpl
import org.apache.flink.table.api.{ApiExpression, Table, TableColumn, TableEnvironment, TableNotExistException, Tumble, TumbleWithSize, TumbleWithSizeOnTime}
import org.apache.flink.table.catalog.exceptions.CatalogException
import org.apache.flink.table.catalog.{Catalog, CatalogBaseTable, Column, ResolvedCatalogTable}
import org.apache.flink.table.operations.{CatalogQueryOperation, QueryOperation}
import org.apache.flink.table.operations.utils.QueryOperationDefaultVisitor
import org.apache.flink.table.types.logical.{BigIntType, TimestampType}
import org.apache.flink.table.types.{AtomicDataType, CollectionDataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.util.UUID
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.concurrent.Future
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

final case class AggrEntry(
                            expr: ApiExpression,
                            flinkUniqueName: String,
                            dType: String
                         ){
  def getAggrExpr = expr.as(flinkUniqueName)
}

object StatsInputsExtractor {
  // FIXME: for now might behave weird, if input is multiple DSes with complex transformations

  def nullSafe(s: String): String = {
    Option(s).getOrElse("[null]")
  }

  def extractTableInputs(t: TableImpl) = {
    logInfo("TableImpl.explain:")
    t.explain()
    // .explainSql
    var inputs = Seq.empty[String]
    val visitor = new QueryOperationDefaultVisitor[Unit](){
      override def defaultMethod(queryOperation: QueryOperation): Unit = {
        logVarWithType(s"Visiting QueryOperation:", queryOperation)
        val childrenNodes = queryOperation.getChildren.asScala
        childrenNodes.foreach(c => c.accept(this))
      }

      override def visit(catalogTable: CatalogQueryOperation): Unit = {
        val tableId = catalogTable.getTableIdentifier
        val tableIdStr = tableId.asSerializableString
        logInfo(s"found CatalogQueryOperation: $tableIdStr")
        // e.g. `default_catalog`.`default_database`.`transactions`
        // it is unresolved input ID at this stage, so try to resolve it
        for {
          cat <- t.getTableEnvironment.getCatalog(tableId.getCatalogName).asScala
          table: CatalogBaseTable <- Try(cat.getTable(tableId.toObjectPath))
            .recoverWith { case NonFatal(ex) =>  logInfo(s"unable to get table: $tableId, ex: ${ex}"); Failure(ex) }
            .toOption
        } yield {
          logVarWithType("table", table)
          val tableOptions = table.getOptions.asScala
          logInfo(
            s"""
               |table.toString: ${table.toString}
               |table.getOptions: ${tableOptions}
               |""".stripMargin)
          table match {
            case resolvedTable: ResolvedCatalogTable if tableOptions.get("connector").contains("kafka") =>
              // table.getOptions: Map(properties.bootstrap.servers -> kafka:9092, format -> csv, topic -> transactions, connector -> kafka)
            (tableOptions.get("topic"), tableOptions.get("properties.bootstrap.servers")) match {
                case (Some(topic), Some(uri)) =>
                  inputs :+= KafkaConn.toKensuDatasourceLocation(topic = topic, kafkaUri = uri)
                case _ =>
              }
          }
        }
      }
    }
    t.getQueryOperation.accept(visitor)
    logInfo(s"resolved input paths for stats input: ${inputs}")
    inputs
  }
}

object KensuStatsHelpers {
  // FIXME
  val NUMERIC_DISTINT_STAT = "distinct_values"

  // FIXME: probably need to know the time columns to aggregate by!!!
  // FIXME: how to know window size!?
  def markStatsInput(timeWindowGroupExpression: TumbleWithSizeOnTime,
                     countDistinctCols: Array[String],
                     //tEnv: TableEnvironment,
                     table: Table): Table = {
    val tableImpl = table match {
      case t: TableImpl => t
      case _ =>
        logInfo(s"markStatsInput: not supported for flink Table which is not TableImpl")
        return table
    }
    extractTableInputs(tableImpl)
    val tEnv = tableImpl.getTableEnvironment
    // how to get original input path automatically?
    val inputId: String = "stat" + UUID.randomUUID().toString.replace("-", "")
    import scala.collection.JavaConverters._
    // based on Table fromResolvedSchema
    val tableSchema = table.getResolvedSchema.getColumns.asScala
    val statAggregations: Seq[AggrEntry] = table.getResolvedSchema.getColumns.asScala.collect {
      case c: Column.PhysicalColumn =>
        val colName = c.getName
        val colDataType = c.getDataType
        // found [PhysicalColumn]: name=account_id dataType: BIGINT dataTypeCls: org.apache.flink.table.types.AtomicDataType`account_id` BIGINT
        // found [PhysicalColumn]: name=transaction_time dataType: TIMESTAMP(3) *ROWTIME* dataTypeCls: org.apache.flink.table.types.AtomicDataType`transaction_time` TIMESTAMP(3) *ROWTIME*
        // found [PhysicalColumn]: name=amount dataType: BIGINT dataTypeCls: org.apache.flink.table.types.AtomicDataType`amount` BIGINT
        logInfo(s"markStatsInput: found [PhysicalColumn]: name=${colName} dataType: ${colDataType} dataTypeCls: ${colDataType.getClass.getCanonicalName}" + c)
        colDataType.getLogicalType match {
          case logicalType @ (_: TimestampType | _: BigIntType) =>
            val dataType = logicalType.asSerializableString
            Seq(
              AggrEntry($(colName).min(), s"${colName}_ksu_min", dataType),
              AggrEntry($(colName).max(), s"${colName}_ksu_max", dataType)
            )
          case _ => Seq.empty
        }
//        c.getName
//        c.getDataType match {
//          case dataType: AtomicDataType => ???
//          case dataType: CollectionDataType => ???
//          case dataType: FieldsDataType => ???
//          case dataType: KeyValueDataType => ???
//          case _ => ???
//        }
        //TableColumn.physical(c.getName, c.getDataType)

      case cxx:  Column.MetadataColumn =>
          logInfo("markStatsInput: input Table contains column of unsupported column type [MetadataColumn]: " + cxx)
          Seq.empty
      case cx:  Column.ComputedColumn =>
        //TableColumn.computed(cx.getName, cx.getDataType, cx.getExpression.asSerializableString)
          logInfo("markStatsInput: input Table contains column of unsupported column type [ComputedColumn]: " + cx)
        Seq.empty
      case column =>
        logInfo("markStatsInput: input Table contains column of unsupported column type: " + column)
        Seq.empty
    }.flatten
    val distinctAggregations: Seq[AggrEntry] = countDistinctCols.flatMap { distinctColName =>
      tableSchema.find(_.getName == distinctColName).map(_.getDataType.getLogicalType).map { dType =>
        // jobmanager_1      | Caused by: org.apache.flink.table.api.ValidationException: Distinct modifier cannot be applied to 'account_id! It can only be applied to an aggregation expression, for example, 'a.count.distinct which is equivalent with COUNT(DISTINCT a).
        AggrEntry($(distinctColName).count().distinct(), s"${distinctColName}_ksu_${NUMERIC_DISTINT_STAT}", dType.asSerializableString)
      }
    }
    val nrows = Seq(
      AggrEntry(lit(1L, new AtomicDataType(new BigIntType(false))).sum(), "_ksu_nrows", (new BigIntType).asSerializableString)
    )
    val allAggregations = (distinctAggregations ++ statAggregations ++ nrows)
    logInfo(allAggregations.toString())
    // FIXME: table.execute()
    // -> feed stats into custom kensu sink simple.
    // -> same for output? can we split stream into two sinks?!!!????  -> for input too!



    // FIXME: we should be able to get input name automagically
    val printTableId = s"print_table_${inputId}"
    // FIXME: generate schema
    val printTableSql = s"""CREATE TABLE ${printTableId} (
                       |  ${allAggregations.map(a => s"${a.flinkUniqueName} ${a.dType}").mkString(",\n")}
                       |) WITH (
                       |  'connector' = 'kensu-stats-reporter'
                       |)""".stripMargin

    // FIXME: can we get tEnv automagically?
    logInfo("printTableSql:" + printTableSql)
    tEnv.executeSql(printTableSql)

    val statsQuery = table
      // https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-tvf/
      // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/tableapi/#overview--examples
      .window(timeWindowGroupExpression.as("statsWindow"))
      .groupBy($("statsWindow"))
      .select(
        allAggregations.map(_.getAggrExpr): _*
      )
      .executeInsert(printTableId)
    // FIXME: shouldn't be doing this?
    import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    tEnv match {
////      case environment: StreamTableEnvironment =>
////        val ds = environment.toDataStream(statsQuery)
////        ds.print(printTableId)
//      case environment: BatchTableEnvironment =>
//        //statsQuery.execute().print()
//        // meh this do not work with Batch environment, only stream
//        //        tEnv
//        //          .execute()
//        //          .process(new ProcessFunction[Row, Row] {
//        //          // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/process_function/
//        //          override def processElement(i: Row, context: ProcessFunction[Row, Row]#Context, collector: Collector[Row]): Unit = {
//        //            collector.collect(i)
//        //          }
//        //        })
//      case environment: TableEnvironment =>
//        Future {
//          statsQuery.execute().print()
//        }(scala.concurrent.ExecutionContext.global) // FIXME: ec?
//        logInfo(s"kensu stats debugging possibly not yet supported for non-stream TableEnvironment: $tEnv")
//    }

    table
  }
}
