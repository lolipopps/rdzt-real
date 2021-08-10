package com.hyt.rtdw.sink.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlGroupDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, envSettings);

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
                ") " +
                "WITH (\n" +
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


        String currencyTableDDL = "CREATE TABLE orders_detail\n" +
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
        tableEnvironment.sqlUpdate(currencyTableDDL);



        String sinkTableDDL = "CREATE TABLE order_cnt (\n" +
                "  log_per_min TIMESTAMP(3),\n" +
                "  item STRING,\n" +
                "  order_cnt BIGINT,\n" +
                "  total_quality BIGINT\n" +
                ") " +
                "WITH (\n" +
                "      'connector' = 'jdbc',\n" +
                "      'username' = 'root',\n" +
                "      'password' = 'hu1234tai',\n" +
                "      'driver' = 'com.mysql.jdbc.Driver',\n" +
                "      'url' = 'jdbc:mysql://172.18.1.6:3306/rtdw?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',\n" +
                "      'table-name' = 'order_cnt'\n" +
                ")\n";
        tableEnvironment.sqlUpdate(sinkTableDDL);

        String querySQL = "insert into order_cnt \n" +
                "select TUMBLE_END(order_time, INTERVAL '10' SECOND),\n" +
                " item, COUNT(order_id) as order_cnt, CAST(sum(amount_kg) as BIGINT) as total_quality\n" +
                "from orders\n" +
                "group by item, TUMBLE(order_time, INTERVAL '10' SECOND)\n" ;


//        String querySQL = "SELECT  t1.order_id \n" +
//                "       ,t1.item \n" +
//                "       ,t1.order_time \n" +
//                "       ,t1.amount \n" +
//                "       ,t2.state \n" +
//                "       ,t2.create_time \n" +
//                "FROM orders t1\n" +
//                "LEFT JOIN orders_detail t2\n" +
//                "ON t1.item = t2.item" ;

        tableEnvironment.sqlUpdate(querySQL);

        System.out.println(querySQL);

        tableEnvironment.execute("StreamKafka2KafkaJob");
    }
}
