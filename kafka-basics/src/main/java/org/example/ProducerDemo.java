package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 10, 2023
 * @since 17
 */
public class ProducerDemo {

    public static void main(String[] args) {
        // 1. Properties
        Properties properties = PropertiesHelper.getInstance().getProperties();
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 2. Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "this message was sent using the sdk");

        // 3. Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(producerRecord);
        producer.flush();
        producer.close();
    }
}