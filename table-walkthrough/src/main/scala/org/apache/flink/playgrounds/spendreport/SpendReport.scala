/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playgrounds.spendreport

import org.apache.flink.integration.kensu.KensuStatsHelpers.TableOps
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
import org.apache.flink.table.api._

object SpendReport {
  val LOG = LoggerFactory.getLogger(this.getClass)

  val statsComputeInterval = 1.minute

  def kensuTrackedTransactionsInput(t: Table) = t
    .select(
      // original input data "select" expression, if any (used by stats)
      // FIXME: what if columns were renamed here, but not only selected!!! ===> rewrite more clearly
      $"tx_kind",
      $"account_id",
      $"transaction_time",
      $"amount"
    ).kensuMarkStatsInput(
      timeWindowGroupExpression = Tumble.over(statsComputeInterval).on($"transaction_time"),
      countDistinctCols         = Array[String]("account_id", "tx_kind") // [optional]
    )

  def aggregateTransactionsTable(transactions: Table): Table =
    kensuTrackedTransactionsInput(transactions)
      .select(
        $"tx_kind",
        $"account_id",
        $"transaction_time".floor(TimeIntervalUnit.HOUR).as("log_ts"),
        $"amount"
      ).groupBy(
        $"tx_kind",
        $"account_id",
        $"log_ts"
      ).select(
        $"tx_kind",
        $"account_id",
        $"log_ts",
        $"amount".sum.as("amount")
      )

  def kafkaLoadTx(tEnv: TableEnvironment, txTable: String) =
    tEnv.executeSql(
      s"""CREATE TABLE ${txTable} (
         |    tx_kind            VARCHAR(100),
         |    account_id         BIGINT,
         |    amount             BIGINT,
         |    transaction_time   TIMESTAMP(3),
         |    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '10' SECOND
         |) WITH (
         |    'connector' = 'kafka',
         |    'topic'     = '${txTable}',
         |    'properties.bootstrap.servers' = 'kafka:9092',
         |    'properties.group.id' = 'flink-stream-reader1',
         |    'scan.startup.mode' = 'earliest-offset',
         |    'properties.auto.offset.reset' = 'earliest',
         |    'format'    = 'csv'
         |)""".stripMargin
    )

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val settings            = EnvironmentSettings.newInstance.inStreamingMode().build // ?
    val tEnv                = TableEnvironment.create(settings)
    LOG.info("Starting job")
    // see https://stackoverflow.com/questions/44668165/org-apache-kafka-clients-consumer-nooffsetforpartitionexception-undefined-offse
    // for scan.startup.mode and properties.auto.offset.reset
    kafkaLoadTx(tEnv, "transactions")
    kafkaLoadTx(tEnv, "aborted_transactions")
    tEnv.executeSql(
      """CREATE TABLE spend_report (
        |    tx_kind          VARCHAR(100),
        |    account_id       BIGINT,
        |    log_ts           TIMESTAMP(3),
        |    spent_amount     BIGINT,
        |    refunded_amount  BIGINT,
        |    final_amount     BIGINT
        |,    PRIMARY KEY (account_id, log_ts) NOT ENFORCED) WITH (
        |  'connector'  = 'jdbc',
        |  'url'        = 'jdbc:mysql://mysql:3306/sql-demo',
        |  'table-name' = 'spend_report',
        |  'driver'     = 'com.mysql.jdbc.Driver',
        |  'username'   = 'sql-demo',
        |  'password'   = 'demo-sql'
        |)""".stripMargin
    )
    val transactions        = tEnv.from("transactions")
    val abortedTransactions = tEnv.from("aborted_transactions")

    val abortedTxAggregated = aggregateTransactionsTable(abortedTransactions).select(
      $("account_id") as "aborted_account_id",
      $("log_ts") as "aborted_log_ts",
      $("amount") as "aborted_amount"
    )

    aggregateTransactionsTable(transactions)
      .leftOuterJoin(
        abortedTxAggregated,
        $"account_id" === $"aborted_account_id" && $"log_ts" === $"aborted_log_ts"
      ).select(
        // beware, column order matters here, not by name!
        $"tx_kind",
        $"account_id",
        $"log_ts",
        $"amount".as("spent_amount"),
        $"aborted_amount".as("refunded_amount"),
        ($"amount" - $"aborted_amount".ifNull(lit(0))).as("final_amount")
      ).kensuExecuteInsert(
        "spend_report",
        // P.S. java syntax: outStatsTimeWindowExpr = Some(Tumble.over(lit(1).day()).on($("log_ts")))
        outStatsTimeWindowExpr = Some(Tumble.over(statsComputeInterval).on($"log_ts"))
        // window - by column/expr or by current timestamp, selected by end-user
        // datastats.timestamp = always currentTimestamp()
      )
  }
}
