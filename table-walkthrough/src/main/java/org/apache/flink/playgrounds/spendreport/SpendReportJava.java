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

package org.apache.flink.playgrounds.spendreport;


import org.apache.flink.integration.kensu.KensuStatsHelpers$;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option$;


import static org.apache.flink.table.api.Expressions.*;

public class SpendReportJava {
    public static Table markStatsInput(
            TumbleWithSizeOnTime timeWindowExpression,
            String[] countDistinctCols,
            Table table) {
        return KensuStatsHelpers$.MODULE$.markStatsInput(timeWindowExpression, countDistinctCols, table);
    }

    public static TableResult kensuExecuteInsert(
            Table tbl,
            String tablePath,
            TumbleWithSizeOnTime outStatsTimeWindowExpr
            ) {
        Boolean overwrite = true;
        return KensuStatsHelpers$.MODULE$.kensuExecuteInsert(
                tbl,
                tablePath,
                overwrite,
                Option$.MODULE$.apply(outStatsTimeWindowExpr),
                new String[]{});
    }

    public static Table report(Table transactions, TableEnvironment tEnv) {
        // FIXME: check what happens when no data received within window!
        return markStatsInput(
                // statistics aggregation interval window =
                Tumble.over(lit(5).minute()).on($("transaction_time")),
                // countDistinctCols =
                new String[]{ "account_id" }, // [optional]
                // original input data "select" expression (used by stats)
                transactions.select(
                $("account_id"),
                $("transaction_time"),
                $("amount"))
        ) // data transformations below:
                .select(
                $("account_id"),
                $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
                $("amount"))
            .groupBy($("account_id"), $("log_ts"))
            .select(
                $("account_id"),
                $("log_ts"),
                $("amount").sum().as("amount"));
    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        // FIXME: changed to StreamTableEnvironment for now, to be able to debug stuff
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        final Logger LOG = LoggerFactory.getLogger(SpendReport.class);
        LOG.info("Starting job");

        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
                "  'table-name' = 'spend_report',\n" +
                "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "  'username'   = 'sql-demo',\n" +
                "  'password'   = 'demo-sql'\n" +
                ")");

        Table transactions = tEnv.from("transactions");
        kensuExecuteInsert(
                report(transactions, tEnv),
                "spend_report",
                Tumble.over(lit(5).minute()).on($("log_ts")));
    }
}
