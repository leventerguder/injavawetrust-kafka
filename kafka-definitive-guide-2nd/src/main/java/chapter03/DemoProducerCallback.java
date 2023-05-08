package chapter03;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoProducerCallback implements Callback {

    private static final Logger log = LoggerFactory.getLogger(DemoProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {

        if (exception == null) {
            log.info("Received new metadata \n" +
                    "Partition: " + metadata.partition() + "\n" +
                    "Topic: " + metadata.topic() + "\n" +
                    "Partition: " + metadata.partition() + "\n" +
                    "Offset: " + metadata.offset() + "\n" +
                    "Timestamp: " + metadata.timestamp() + "\n"
            );
        }
        if (exception != null) {
            exception.printStackTrace();
        }
    }
}
