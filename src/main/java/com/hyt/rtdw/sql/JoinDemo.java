package com.hyt.rtdw.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);
        String orderTableDDL = "CREATE TABLE orders (\n" +
                "  order_id STRING,\n" +
                "  item    STRING,\n" +
                "  currency STRING,\n" +
                "  amount DOUBLE,\n" +
                "  order_time TIMESTAMP(3),\n" +
                "  proc_time as PROCTIME(),\n" +
                "  amount_kg as amount * 1000\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.topic' = 'order_table',\n" +
                "  'connector.version'='universal',\n" +
                "  'connector.properties.zookeeper.connect' = '47.100.71.215:2181',\n" +
                "  'connector.properties.bootstrap.servers' = '47.100.71.215:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")\n";
        String currencyTableDDL = "CREATE TABLE orders_detail\n" +
                "(\n" +
                "    order_id   STRING,\n" +
                "    item       STRING,\n" +
                "    status   STRING,\n" +
                "    create_time TIMESTAMP(3)\n" +
                ") with ('connector.type' = 'kafka',\n" +
                "      'connector.version' = 'universal',\n" +
                "      'connector.properties.group.id' = 'g2.group1',\n" +
                "      'connector.properties.bootstrap.servers' = '47.100.71.215:9092',\n" +
                "      'connector.properties.zookeeper.connect' = '47.100.71.215:2181',\n" +
                "      'connector.topic' = 'orders_detail',\n" +
                "      'connector.startup-mode' = 'latest-offset',\n" +
                "      'format.type' = 'json',\n" +
                "      'format.derive-schema' = 'true'\n" +
                "      )\n";

        String querySQL = "SELECT  t1.order_id \n" +
                "       ,t1.item \n" +
                "       ,t1.order_time \n" +
                "       ,t1.amount \n" +
                "       ,t2.status \n" +
                "       ,t2.create_time \n" +
                "FROM orders t1\n" +
                "LEFT JOIN orders_detail t2\n" +
                "ON t1.order_id = t2.order_id" ;
        tableEnvironment.sqlUpdate(orderTableDDL);
        tableEnvironment.sqlUpdate(currencyTableDDL);
        tableEnvironment.executeSql(querySQL).print();
        tableEnvironment.execute("StreamKafka2KafkaJob");
    }
}
