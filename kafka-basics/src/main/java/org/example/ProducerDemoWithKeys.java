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
public class ProducerDemoWithKeys {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        Properties properties = PropertiesHelper.getInstance().getProperties();
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord;

        String topic = "demo_java";

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 10; j++) {
                String key = "id_" + j;
                String value = "hello world " + j * i;

                producerRecord = new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, (recordMetadata, ex) -> {
                    if (ex == null) {
                        LOG.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                    } else {
                        LOG.error(ex.getMessage(), ex);
                    }
                });
            }
        }

        producer.flush();
        producer.close();
    }
}