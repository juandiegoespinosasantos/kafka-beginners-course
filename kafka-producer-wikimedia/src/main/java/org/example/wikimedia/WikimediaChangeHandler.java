package org.example.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 12, 2023
 * @since 17
 */
public class WikimediaChangeHandler implements BackgroundEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // Nothing to do...
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        String data = messageEvent.getData();
        LOG.info(data);

        kafkaProducer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void onComment(String comment) {
        // Nothing to do...
    }

    @Override
    public void onError(Throwable th) {
        LOG.error(th.getMessage(), th);
    }
}