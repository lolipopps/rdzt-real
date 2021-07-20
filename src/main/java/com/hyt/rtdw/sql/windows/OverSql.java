package com.hyt.rtdw.sql.windows;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;



public class OverSql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);



        String orderTableDDL = "CREATE TABLE orders (\n" +
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
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")\n";

        String orderDetailTableDDL = "CREATE TABLE orders_detail\n" +
                "(\n" +
                "    order_id   STRING,\n" +
                "    item       STRING,\n" +
                "    state   STRING,\n" +
                "    create_time TIMESTAMP(3)\n" +
                ") with ('connector.type' = 'kafka',\n" +
                "      'connector.version' = 'universal',\n" +
                "      'connector.properties.group.id' = 'g2.group1',\n" +
                "      'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "      'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "      'connector.topic' = 'orders_detail',\n" +
                "      'connector.startup-mode' = 'latest-offset',\n" +
                "      'format.type' = 'json',\n" +
                "      'format.derive-schema' = 'true'\n" +
                "      )\n";
        tableEnvironment.sqlUpdate(orderTableDDL);

        String querySQL =
                "SELECT  order_id,item \n" +
                        "       ,order_time \n" +
                        "       ,amount \n" +
                        "       ,ROW_NUMBER() OVER ( PARTITION BY item ORDER BY order_time desc) AS rownum\n" +
                        "FROM orders";
        tableEnvironment.executeSql(querySQL).print();
        tableEnvironment.execute("执行");

    }

}
