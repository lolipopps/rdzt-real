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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class JsonOrderSender {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();
    private static final SendCallBack sendCallBack = new SendCallBack();
    private static final String topicName = "order_table";
    private static final List<String> currencies = initCurrencies();
    private static final List<String> itemNames = initItemNames();

    public static synchronized void sendMessage(Properties kafkaProperties, int continueMinutes) throws InterruptedException, JsonProcessingException {
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(kafkaProperties);
        // order stream
        for (int i = 0; i < continueMinutes * 60; i++) {
            long timestart = System.currentTimeMillis();
//            for (int j = 0; j < currencies.size(); j++) {
                Map<String, Object> map = new HashMap<>();
                map.put("order_id", i);
                map.put("item", itemNames.get(random.nextInt(itemNames.size()) % itemNames.size()));
                map.put("currency", currencies.get(random.nextInt(currencies.size()) % currencies.size()));
                map.put("amount", random.nextInt(10) % 100 / 100.0);
                Long time = System.currentTimeMillis();
                DateFormat dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                Date date = new Date(time);
                String jsonSchemaDate = dateFormat.format(date);
                map.put("order_time", jsonSchemaDate);
                map.put("ts_time", time);
                producer.send(new ProducerRecord<>(
                                topicName,
                                String.valueOf(time),
                                objectMapper.writeValueAsString(map)
                        ), sendCallBack

                );
                System.out.println( objectMapper.writeValueAsString(map));
                Thread.sleep(10000);

//            }
            long timecast = System.currentTimeMillis() - timestart;
            System.out.println((i + 1) * currencies.size() + " has sended to topic:[" + topicName + "] in " + timecast + "ms");
            if (timecast < 2000) {
                System.out.println("begin sleep...." + System.currentTimeMillis());
                Thread.sleep(2000);
                System.out.println("end sleep...." + System.currentTimeMillis());

            }
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
