package com.hyt.rtdw.data;

import com.hyt.rtdw.util.DataGenUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class JsonOrderDetailSender {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();
    private static final SendCallBack sendCallBack = new SendCallBack();
    private static final String topicName = "orders_detail";
    private static final List<String> currencies = initCurrencies();
    private static final List<String> itemNames = initItemNames();
    private static final List<String> states = initItStates();

    public static synchronized void sendMessage(Properties kafkaProperties, int continueMinutes) throws InterruptedException, JsonProcessingException {
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(kafkaProperties);
        // order stream
        for (int i = 0; i < continueMinutes * 60; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("order_id", DataGenUtil.getRandomNumber(1, 1));
            map.put("item", itemNames.get(random.nextInt(itemNames.size()) % itemNames.size()));
            map.put("state", states.get(random.nextInt(states.size()) % states.size()));
            map.put("id", DataGenUtil.getString(10));
            Long time = System.currentTimeMillis();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            Date date = new Date(time);
            String jsonSchemaDate = dateFormat.format(date);
            map.put("create_time", jsonSchemaDate);
            producer.send(new ProducerRecord<>(
                            topicName,
                            String.valueOf(time),
                            objectMapper.writeValueAsString(map)
                    ), sendCallBack

            );
            System.out.println("orders_detail: " + objectMapper.writeValueAsString(map));
            Thread.sleep(5000);

        }
    }

    static class SendCallBack implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private static List<String> initCurrencies() {
        final List<String> currencies = new ArrayList<>();
        currencies.add("US Dollar");
        currencies.add("Euro");
        currencies.add("Yen");
        currencies.add("人民币");
        return currencies;
    }

    private static List<String> initItStates() {
        final List<String> itermNames = new ArrayList<>();
        itermNames.add("有效");
        itermNames.add("无效");
        itermNames.add("运输中");
        itermNames.add("生成订单");
        itermNames.add("订单完结");
        return itermNames;
    }

    private static List<String> initItemNames() {
        final List<String> itermNames = new ArrayList<>();
        itermNames.add("Apple");
        itermNames.add("橘子");
        itermNames.add("Paper");
        itermNames.add("牛奶");
        itermNames.add("酸奶");
        itermNames.add("豆腐");
        return itermNames;
    }
}
