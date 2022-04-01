package org.apache.flink.integration.kensu

import org.apache.flink.integration.kensu.KensuFlinkHook.logInfo
import org.apache.flink.table.api.Expressions.{$, lit}
import org.apache.flink.table.api.{ApiExpression, Table, TableColumn, TableEnvironment}
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.types.logical.{BigIntType, TimestampType}
import org.apache.flink.table.types.{AtomicDataType, CollectionDataType, FieldsDataType, KeyValueDataType}
import org.apache.flink.types.Row

final case class AggrEntry(
                            expr: ApiExpression,
                            flinkUniqueName: String,
                            dType: String
                         ){
  def getAggrExpr = expr.as(flinkUniqueName)
}

object KensuStatsHelpers {
  // FIXME
  val NUMERIC_DISTINT_STAT = "distinct_values"

  // FIXME: probably need to know the time columns to aggregate by!!!
  // FIXME: how to know window size!?
  def markStatsInput(timeWindowGroupExpression: ApiExpression,
                     countDistinctCols: Array[String],
                     inputId: String,
                     tEnv: TableEnvironment,
                     table: Table): Table = {
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
                       |  'connector' = 'print'
                       |)""".stripMargin

    // FIXME: can we get tEnv automagically?
    logInfo("printTableSql:" + printTableSql)
    tEnv.executeSql(printTableSql)

    val statsQuery = table
      .groupBy(timeWindowGroupExpression)
      .select(
        allAggregations.map(_.getAggrExpr): _*
      ) //.executeInsert(printTableId)
    // FIXME: shouldn't be doing this?
    import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    val ds = tableEnv.toDataStream(statsQuery)
    ds.print("printTableId")

    table
  }
}

