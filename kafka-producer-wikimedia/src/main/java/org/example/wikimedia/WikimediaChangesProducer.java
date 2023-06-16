package org.example.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 12, 2023
 * @since 17
 */
public class WikimediaChangesProducer {

    private static final String PROPERTY_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String EVENT_SOURCE_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    private static final String TOPIC = "wikimedia.recentchange";

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = buildKafkaProducer();

        BackgroundEventSource eventSource = buildEventHandler(producer);
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }

    private static KafkaProducer<String, String> buildKafkaProducer() {
        return new KafkaProducer<>(getProperties());
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PROPERTY_BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1_024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return properties;
    }

    private static BackgroundEventSource buildEventHandler(final KafkaProducer<String, String> producer) {
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, TOPIC);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(EVENT_SOURCE_URL));

        return new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder).build();
    }
}