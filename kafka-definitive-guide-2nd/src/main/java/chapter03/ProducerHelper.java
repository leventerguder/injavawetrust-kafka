package chapter03;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerHelper {

    private static Properties createKafkaProperties() {

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.serializer", StringSerializer.class);
        kafkaProperties.put("value.serializer", StringSerializer.class);

        return kafkaProperties;

    }

    public static Producer createKafkaProducer() {
        Properties kafkaProperties = createKafkaProperties();
        return new KafkaProducer<>(kafkaProperties);
    }

    public static ProducerRecord<String, String> createProducerRecord() {
        return new ProducerRecord<>("CustomerCountry",
                "Precision Products",
                "France");
    }
}
