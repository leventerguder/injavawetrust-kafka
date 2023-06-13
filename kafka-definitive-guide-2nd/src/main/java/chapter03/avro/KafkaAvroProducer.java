package chapter03.avro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaAvroProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");
        // schema.registry.url is the configuration of the Avro serializer that will be passed to the serializer by the producer.
        // It simply points to where we store the schemas.

        String topic = "customerContacts";
        Producer<String, Customer> producer = new KafkaProducer<>(props);

        while (true) {
            Customer customer = new Customer();
            // Customer class is not a regular Java class, but rather a specialized Avro object, generated from a schema using Avro code generation.
            // Avro classes can only serialize Avro objects not POJO.
            // Generating Avro classes can be done either using the avro-tools.jar or the Avro maven plugin.
            customer.setId(new Random().nextInt(500));
            customer.setName("levent");
            customer.setEmail("a@b.com");
            System.out.println("Generated customer " + customer);

            ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, customer.getName(), customer);
            producer.send(record);

        }
    }
}
