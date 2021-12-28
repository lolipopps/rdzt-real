package com.hyt.rtdw.sql.windows;

import com.alibaba.fastjson.JSONObject;
import com.hyt.rtdw.config.KafkaConfig;
import com.hyt.rtdw.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import java.util.Set;


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
                "  amount_kg as amount * 1000\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.topic' = 'order_table',\n" +
                "  'connector.version'='universal',\n" +
                "  'connector.properties.zookeeper.connect' = '172.18.1.11:2181',\n" +
                "  'connector.properties.bootstrap.servers' = '172.18.1.21:9092',\n" +
                "  'connector.properties.group.id' = 'testGroup3',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")\n";

        String orderDetailTableDDL = "CREATE TABLE orders_detail\n" +
                "(\n" +
                "    order_id   STRING,\n" +
                "    id       STRING,\n" +
                "    status   STRING,\n" +
                "    create_time TIMESTAMP(3)\n" +
                ") with ('connector.type' = 'kafka',\n" +
                "      'connector.version' = 'universal',\n" +
                "      'connector.properties.group.id' = 'g2.group1',\n" +
                "      'connector.properties.bootstrap.servers' = '172.18.1.21:9092',\n" +
                "      'connector.properties.zookeeper.connect' = '172.18.1.11:2181',\n" +
                "      'connector.topic' = 'orders_detail',\n" +
                "      'connector.startup-mode' = 'latest-offset',\n" +
                "      'format.type' = 'json',\n" +
                "      'format.derive-schema' = 'true'\n" +
                "      )\n";
        tableEnvironment.sqlUpdate(orderTableDDL);
        tableEnvironment.sqlUpdate(orderDetailTableDDL);

        String querySQL =
                        " SELECT t1.order_id, t1.item, t1.order_time, t2.create_time,t2.status\n" +
                        "from (\n" +
                        "         SELECT order_id\n" +
                        "              , item\n" +
                        "              , order_time\n" +
                        "              , amount\n" +
                        "              , ROW_NUMBER() OVER ( PARTITION BY order_id ORDER BY order_time desc) AS rownum\n" +
                        "         FROM orders\n" +
                        "     ) t1\n" +
                        "         left join\n" +
                        "     (\n" +
                        "         SELECT order_id\n" +
                        "              , id\n" +
                        "              , create_time\n" +
                        "              , status\n" +
                        "              , ROW_NUMBER() OVER ( PARTITION BY order_id ORDER BY create_time desc) AS rownum\n" +
                        "         FROM orders_detail\n" +
                        "     ) t2\n" +
                        "     on t1.order_id = t2.order_id\n" +
                        "WHERE t1.rownum = 1 and t2.rownum = 1\n ";
        Table table = tableEnvironment.sqlQuery(querySQL);

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnvironment.toRetractStream(table, Row.class);


        tuple2DataStream.flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, JSONObject>() {
            @Override
            public void flatMap(Tuple2<Boolean, Row> value, Collector<JSONObject> out) throws Exception {
                Boolean lastValue = value.f0;
                if(lastValue) {
                    Row row = value.f1;
                    Set<String> names = row.getFieldNames(true);
                    JSONObject json = new JSONObject();
                    for (String name : names) {
                        json.put(name, row.getField(name));
                    }
                    out.collect(json);
                }
            }
        }).print();
//        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
//                "flink_sink",
//                new SimpleStringSchema(),
//                KafkaConfig.buildKafkaProps());   // 序列化 schema
//        myProducer.setWriteTimestampToKafka(true);
//
//        sinkKafka.addSink(myProducer).name("ods").uid("ods").setParallelism(1);

        //todo  保留最后一条最新的数据
       /* tuple2DataStream.flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, JSONObject>() {
            @Override
            public void flatMap(Tuple2<Boolean, Row> value, Collector<JSONObject> out) throws Exception {
                Boolean lastValue = value.f0;
                Row row = value.f1;

                JSONObject json = new JSONObject();
                json.put("state",lastValue);
                json.put("ts",row.getField(0));
                json.put("user_id",row.getField(1));
                json.put("behavior",row.getField(2));
                json.put("row_num",row.getField(3));
                out.collect(json);
            }
        }).print();*/

////        insert into kafka_table2
//        sinkKafka.print();
        env.execute("123");

    }

}
