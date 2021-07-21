package com.hyt.rtdw.util;

import com.hyt.rtdw.config.KafkaConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
@Slf4j
public class KafkaUtil {

    static class MyKafkaProducer implements Runnable {

        KafkaProducer producer;
        String topic;

        MyKafkaProducer(String topic){
            Properties props = null;
            props = KafkaConfig.buildKafkaProps();
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            this.producer = new KafkaProducer<String, String>(props);
            this.topic = topic;

        }
        @SneakyThrows
        @Override
        public void run() {
            int num = 1;
            while (true) {
                String a = DataGenUtil.getString(10);
                log.info("发送数据: " + a);
                ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, "dasdasd");
                producer.send(record);
                Thread.sleep(100);
                producer.flush();
                num++;
                if(num == 1000){
                    break;
                }
            }
        }
    }

    public static void readFromKafka(String topic) throws InterruptedException {
        Properties props = null;
        props = KafkaConfig.buildKafkaProps();
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("---------开始消费---------");
        while (true) {
            int messageNo = 1;
            ConsumerRecords<String, String> msgList = consumer.poll(1000);
            try {
                if (null != msgList && msgList.count() > 0) {
                    for (ConsumerRecord<String, String> record : msgList) {
                        //消费100条就打印 ,但打印的数据不一定是这个规律的
                        System.out.println("---------开始消费---------： " + messageNo);
                        if (messageNo % 2 == 0) {
                            System.out.println(messageNo + "=======receive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());
                        }
                        //当消费了1000条就退出
                        if (messageNo % 100 == 0) {
                            break;
                        }
                        messageNo++;
                    }
                } else {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                consumer.close();

            }

        }
    }


    public static void main(String[] args) throws InterruptedException {
        MyKafkaProducer p = new MyKafkaProducer("ods_showing_showings_rt_si");
        readFromKafka("ods_showing_showings_rt_si");
    }
}
