package com.hyt.rtdw.sink.mysql;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Set;


public class MysqlJoinSql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

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
        tableEnvironment.sqlUpdate(orderDetailTableDDL);

        String sinkTableDDL = "CREATE TABLE join_order (\n" +
                "  order_id STRING,\n" +
                "  item STRING primary key,\n" +
                "  order_time TIMESTAMP(3),\n" +
                "  create_time TIMESTAMP(3),\n" +
                "  state STRING\n" +
                ")" +
                " WITH (\n" +
                "      'connector' = 'jdbc',\n" +
                "      'username' = 'root',\n" +
                "      'password' = 'hu1234tai',\n" +
                "      'driver' = 'com.mysql.jdbc.Driver',\n" +
                "      'url' = 'jdbc:mysql://172.18.1.6:3306/rtdw?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',\n" +
                "      'table-name' = 'order_join_detail'\n" +
                ")";
        tableEnvironment.sqlUpdate(sinkTableDDL);
        String querySQL =
                " insert into join_order " +
                        "SELECT t1.order_id, t1.item, t1.order_time, t2.create_time,t2.state\n" +
                        "from (\n" +
                        "         SELECT order_id\n" +
                        "              , item\n" +
                        "              , order_time\n" +
                        "              , amount\n" +
                        "              , ROW_NUMBER() OVER ( PARTITION BY item ORDER BY order_time desc) AS rownum\n" +
                        "         FROM orders\n" +
                        "     ) t1\n" +
                        "         left join\n" +
                        "     (\n" +
                        "         SELECT order_id\n" +
                        "              , item\n" +
                        "              , create_time\n" +
                        "              , state\n" +
                        "              , ROW_NUMBER() OVER ( PARTITION BY item ORDER BY create_time desc) AS rownum\n" +
                        "         FROM orders_detail\n" +
                        "     ) t2\n" +
                        "     on t1.item = t2.item\n" +
                        "WHERE t1.rownum = 1 and t2.rownum = 1\n ";
        tableEnvironment.sqlUpdate(querySQL);
        tableEnvironment.execute("123");

    }

}
