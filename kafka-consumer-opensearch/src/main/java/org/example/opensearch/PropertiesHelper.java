package org.example.opensearch;

import java.util.Properties;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 10, 2023
 * @since 11
 */
public class PropertiesHelper {

    private static PropertiesHelper instance;

    private static final String PROPERTY_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String PROPERTY_SECURITY_PROTOCOL = "SASL_SSL";
    private static final String PROPERTY_SASL_JAAS_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2wO14f1k0Iax6EztpUaCav\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyd08xNGYxazBJYXg2RXp0cFVhQ2F2Iiwib3JnYW5pemF0aW9uSWQiOjczNzg2LCJ1c2VySWQiOjg1ODE1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhNWI5NmUyOS04YzJlLTQyNTUtYjUzMC1hYWYxODc0MDFkMmYifX0.rrfJqw65hPqLXapD-LN484Kx7haySpo8fFRdqyAOJEQ\";";
    private static final String PROPERTY_SASL_MECHANISM = "PLAIN";

    private final Properties properties;

    private PropertiesHelper() {
        properties = new Properties();

        // Connect to server
        properties.setProperty("bootstrap.servers", PROPERTY_BOOTSTRAP_SERVERS);
//        properties.setProperty("security.protocol", PROPERTY_SECURITY_PROTOCOL);
//        properties.setProperty("sasl.jaas.config", PROPERTY_SASL_JAAS_CONFIG);
//        properties.setProperty("sasl.mechanism", PROPERTY_SASL_MECHANISM);
    }

    public Properties getProperties() {
        return properties;
    }

    public static PropertiesHelper getInstance() {
        if (instance == null) instance = new PropertiesHelper();

        return instance;
    }
}