package org.apache.flink.integration.kensu

import org.apache.flink.integration.kensu.KensuFlinkHook.logInfo
import org.apache.flink.table.api.Expressions.{$, lit}
import org.apache.flink.table.api.{ApiExpression, Table, TableColumn}
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.types.logical.{BigIntType, TimestampType}
import org.apache.flink.table.types.{AtomicDataType, CollectionDataType, FieldsDataType, KeyValueDataType}

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
            Seq(
              AggrEntry($(colName).min(), s"${colName}_ksu_min", logicalType.toString),
              AggrEntry($(colName).max(), s"${colName}_ksu_max", logicalType.toString)
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
        AggrEntry($(distinctColName).count().distinct(), s"${distinctColName}_ksu_${NUMERIC_DISTINT_STAT}", dType.toString)
      }
    }
    val nrows = Seq(
      AggrEntry(lit(1L, new AtomicDataType(new BigIntType(false))).sum(), "_ksu_nrows", (new BigIntType).toString)
    )
    val allAggregations = (distinctAggregations ++ statAggregations ++ nrows)
    logInfo(allAggregations.toString())
    // FIXME: table.execute()
    // -> feed stats into custom kensu sink simple.
    // -> same for output? can we split stream into two sinks?!!!????  -> for input too!



    // FIXME: we should be able to get input name automagically
    val printTableId = s"print_table_${inputId}"
    // FIXME: generate schema
    val printTable = s"""CREATE TABLE ${printTableId} (
                       |  ${allAggregations.map(a => s"${a.flinkUniqueName} ${a.dType}").mkString(",\n")}
                       |) WITH (
                       |  'connector' = 'print'
                       |);""".stripMargin

    table
      .groupBy(timeWindowGroupExpression)
      .select(
        allAggregations.map(_.getAggrExpr): _*
      ).executeInsert(printTableId)

    table
  }
}

