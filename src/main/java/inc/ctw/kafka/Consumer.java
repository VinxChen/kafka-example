package inc.ctw.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "b-2.g123-kafka-common.q56iz5.c2.kafka.ap-northeast-1.amazonaws.com:9092,b-1.g123-kafka-common.q56iz5.c2.kafka.ap-northeast-1.amazonaws.com:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-local-consumer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList("biz_payment_v3"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic: %s, offset: %d, msg: %s", record.topic(), record.offset(), record.value()));
            }
        }
    }
}
