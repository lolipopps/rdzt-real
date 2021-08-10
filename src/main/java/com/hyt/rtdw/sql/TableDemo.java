package com.hyt.rtdw.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        String sourceTableDDL = "CREATE TABLE orders (\n" +
                "  order_id STRING,\n" +
                "  item    STRING,\n" +
                "  currency STRING,\n" +
                "  amount DOUBLE,\n" +
                "  order_time TIMESTAMP(3),\n" +
                "  proc_time as PROCTIME(),\n" +
                "  amount_kg as amount * 1000,\n" +
                "  ts as order_time + INTERVAL '1' SECOND,\n" +
                "  WATERMARK FOR order_time AS order_time" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.topic' = 'order_table',\n" +
                "  'connector.version'='universal',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'tableDemo',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")\n";
        tableEnvironment.sqlUpdate(sourceTableDDL);

        String sinkTableDDL = "CREATE TABLE order_cnt (\n" +
                "  item STRING,\n" +
                "  order_cnt BIGINT,\n" +
                "  total_quality BIGINT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version'='universal',\n" +
                "  'connector.topic' = 'order_cnt',\n" +
                "  'update-mode' = 'append',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySQL = "SELECT  TUMBLE_END(order_time,INTERVAL '10' SECOND) \n" +
                "       ,item \n" +
                "       ,COUNT(order_id)                AS order_cnt \n" +
                "       ,CAST(SUM(amount_kg) AS BIGINT) AS total_quality\n" +
                "FROM orders\n" +
                "GROUP BY  item \n" +
                "         ,TUMBLE (order_time,INTERVAL '10' SECOND )" ;
        tableEnvironment.executeSql(querySQL).print();
        env.execute("执行");

    }

}
