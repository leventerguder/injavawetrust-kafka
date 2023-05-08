package chapter03;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer01 {

    public static void main(String[] args) {

        Producer producer = ProducerHelper.createKafkaProducer();
        ProducerRecord producerRecord = ProducerHelper.createProducerRecord();

        try {
            producer.send(producerRecord); // fire-and-forget
        } catch (Exception e) {
            // SerializationException
            // ExhaustedException
            // TimeoutException
            e.printStackTrace();
        }

        // The simplest way to send a message synchronously is as follows:
        try {
            // Here, we are using Future.get() to wait for a reply from Kafka.
            Object recordMetaData = producer.send(producerRecord).get();
            System.out.println(recordMetaData.getClass()); // RecordMetadata
        } catch (Exception e) {
            // SerializationException
            // ExhaustedException
            // TimeoutException
            e.printStackTrace();
        }

        // Sending a Message Asynchronously
        try {
            producer.send(producerRecord, new DemoProducerCallback()).get();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
