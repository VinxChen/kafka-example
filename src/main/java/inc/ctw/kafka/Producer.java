package inc.ctw.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args) {
        String topic = "biz_payment_v3";
        String brokers = "b-2.g123-kafka-common.q56iz5.c2.kafka.ap-northeast-1.amazonaws.com:9092,b-1.g123-kafka-common.q56iz5.c2.kafka.ap-northeast-1.amazonaws.com:9092";

        String localTopic = "biz_payment_event";
        String localBrokers = "127.0.0.1:9092";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        try {
            while (true) {
                String msg = "{\"type\": \"p_payfailed\", \"ctwid\": \"456\", \"appid\": \"456\"}";
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
                kafkaProducer.send(record);
                System.out.println("send success: " + msg);
                Thread.sleep(3000);
            }
        } catch (InterruptedException e) {
            //
        } finally {
            kafkaProducer.close();
        }

    }
}
