package chapter03.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaAvroProducerV2 {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");
        // schema.registry.url is the configuration of the Avro serializer that will be passed to the serializer by the producer.
        // It simply points to where we store the schemas.

        String topic = "customerContacts";

        String schemaString = """
                {
                  "namespace": "chapter03.avro",
                  "type": "record",
                  "name": "CustomerV2",
                  "fields": [
                    {
                      "name": "id",
                      "type": "int"
                    },
                    {
                      "name": "name",
                      "type": "string"
                    },
                    {
                      "name": "email",
                      "type": [
                        "null",
                        "string"
                      ]
                    }
                  ]
                }
                """;

        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
        // Avro GenericRecord, which we initialize with our schema and the data we want to write.

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        while (true) {

            GenericRecord customer = new GenericData.Record(schema);
            customer.put("id", new Random().nextInt(500));
            customer.put("name", "levent");
            customer.put("email", "erguder.levent@gmail.com");
            System.out.println("Generated customer " + customer);


            ProducerRecord<String, GenericRecord> data =
                    new ProducerRecord<>("customerContacts-v2", "levent", customer);

            // ProducerRecord is simply a GenericRecord that contains our schema and data.
            // The serializer will know how to get the schema from this record, store it in the Schema Registry,
            // and serializer the object data.
            producer.send(data);

        }
    }
}
