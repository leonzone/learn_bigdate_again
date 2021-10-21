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

package com.reiser.flink.sql.sendreport;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import static org.apache.flink.table.api.Expressions.$;

public class SpendReport {

    public static Table report(Table transactions) {
        return transactions.select(
                $("account_id"),
                $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
                $("amount"))
            .groupBy($("account_id"), $("log_ts"))
            .select(
                $("account_id"),
                $("log_ts"),
                $("amount").sum().as("amount"));
    }

//    public static Table report(Table transactions) {
//        return transactions
////                .window(Tumble.over(lit(1).hour().minute()).on($("transaction_time")).as("log_ts"))
//                .window(Slide.over(lit(5).hour()).every(lit(1).hour()).on($("transaction_time")).as("log_ts"))
//                .groupBy($("account_id"), $("log_ts"))
//                .select(
//                        $("account_id"),
//                        $("log_ts").start().as("log_ts"),
//                        $("amount").avg().as("amount"));
//    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        //输入表transaction，用于读取信用卡交易数据，其中包含账户ID(account_id)，美元金额和时间戳
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
        //输出表spend_report存储聚合结果，是mysql表
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
        //将transactions表经过report函数处理后写入到spend_report表
        Table transactions = tEnv.from("transactions");
        report(transactions).executeInsert("spend_report");
    }
}
