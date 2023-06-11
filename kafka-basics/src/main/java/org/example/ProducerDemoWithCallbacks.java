package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 10, 2023
 * @since 17
 */
public class ProducerDemoWithCallbacks {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        // 1. Properties
        Properties properties = PropertiesHelper.getInstance().getProperties();
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 2. Record

        // 3. Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord;

        for (int i = 0; i < 10; i++) {
            producerRecord = new ProducerRecord<>("demo_java", "demo #" + (i * 10));

            producer.send(producerRecord, (recordMetadata, ex) -> {
                if (ex == null) {
                    LOG.info("Received new metadata \n" + "Topic: " + recordMetadata.topic() + "\n" + "Partition: " + recordMetadata.partition() + "\n" + "Offset: " + recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp() + "\n");
                } else {
                    LOG.error(ex.getMessage(), ex);
                }
            });

            Thread.sleep(2_000L);
        }

        producer.flush();
        producer.close();
    }
}