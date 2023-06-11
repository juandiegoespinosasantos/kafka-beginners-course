package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 10, 2023
 * @since 11
 */
public class ConsumerDemoCooperative {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = PropertiesHelper.getInstance().getProperties();
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread thread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("Shutdown detected...");

                consumer.wakeup();

                try {
                    thread.join();
                } catch (InterruptedException ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));

            ConsumerRecords<String, String> records;

            while (true) {
                LOG.info("Polling...");

                records = consumer.poll(Duration.ofMillis(1_000L));

                for (ConsumerRecord<String, String> record : records) {
                    LOG.info(record.key() + " = " + record.value());
                    LOG.info("Partition = " + record.partition() + "\tOffset = " + record.offset());
                    LOG.info("");
                }
            }
        } catch (WakeupException ex) {
            LOG.info("Consumer shutting down...");
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            consumer.close();
        }
    }
}
