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

    private static final String PROPERTY_BOOTSTRAP_SERVERS = "cluster.playground.cdkt.io:9092";
    private static final String PROPERTY_SECURITY_PROTOCOL = "SASL_SSL";
    private static final String PROPERTY_SASL_JAAS_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2wO14f1k0Iax6EztpUaCav\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyd08xNGYxazBJYXg2RXp0cFVhQ2F2Iiwib3JnYW5pemF0aW9uSWQiOjczNzg2LCJ1c2VySWQiOjg1ODE1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhNWI5NmUyOS04YzJlLTQyNTUtYjUzMC1hYWYxODc0MDFkMmYifX0.rrfJqw65hPqLXapD-LN484Kx7haySpo8fFRdqyAOJEQ\";";
    private static final String PROPERTY_SASL_MECHANISM = "PLAIN";

    public static void main(String[] args) {
        // 1. Properties
        Properties properties = getProperties();

        // 2. Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "this message was sent using the sdk");

        // 3. Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(producerRecord);
        producer.flush();
        producer.close();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();

        // Connect to server
        properties.setProperty("bootstrap.servers", PROPERTY_BOOTSTRAP_SERVERS);
        properties.setProperty("security.protocol", PROPERTY_SECURITY_PROTOCOL);
        properties.setProperty("sasl.jaas.config", PROPERTY_SASL_JAAS_CONFIG);
        properties.setProperty("sasl.mechanism", PROPERTY_SASL_MECHANISM);

        // Producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return properties;
    }
}