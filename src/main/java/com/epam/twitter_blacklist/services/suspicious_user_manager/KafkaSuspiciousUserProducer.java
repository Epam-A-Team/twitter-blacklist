package com.epam.twitter_blacklist.services.suspicious_user_manager;

import com.epam.twitter_blacklist.models.SuspiciousActivity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaSuspiciousUserProducer {

    @SneakyThrows
    public static void main(String[] args) {

        String topic = "user-suspicious-activity-topic";
        String bootstrapServer = "34.134.45.229:9092";
        int chuckMessageCount = 100;
        int numOfChunks = 10;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        for (int chunk = 0; chunk < numOfChunks; chunk++){
            for (int messageIdx = 1; messageIdx <= chuckMessageCount; messageIdx++){
                int messageNumber = (chuckMessageCount * chunk) + messageIdx;
                SuspiciousActivity appl = SuspiciousActivity.builder().userFullName("God").userAddress("Tel-Aviv").messageDate("01012000").message("I want's to kill ya! *" + chunk + "*" + messageIdx + "*").category("Murder").build();
                ObjectMapper Obj = new ObjectMapper();

                String messageContent = Obj.writeValueAsString(appl);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, messageContent);

                System.out.println(record.value());
                producer.send(record);
            }

            producer.flush();
            Thread.sleep(2000);
        }

        producer.close();

    }
}