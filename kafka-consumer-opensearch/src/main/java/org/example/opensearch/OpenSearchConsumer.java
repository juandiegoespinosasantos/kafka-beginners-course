package org.example.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 16, 2023
 * @since 17
 */
public class OpenSearchConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        // Create Kafka client
        // Create OpenSearch client
        try (RestHighLevelClient openSearchClient = createOpenSearchClient();
             KafkaConsumer<String, String> consumer = createKafkaConsumer()) {
            String index = "wikimedia";
            IndicesClient indices = openSearchClient.indices();

            boolean exists = indices.exists(new GetIndexRequest(index), RequestOptions.DEFAULT);

            // Create index if it doesn't exist
            if (exists) {
                LOG.info("{} index already exists!", index);
            } else {
                indices.create(new CreateIndexRequest(index), RequestOptions.DEFAULT);

                LOG.info("{} index has been created!", index);
            }

            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3_000));

                LOG.info("Received {} records", records.count());

                for (ConsumerRecord<String, String> record : records) {
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    String id = extractId(record.value());

                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .id(id)
                                .source(record.value(), XContentType.JSON);
                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        LOG.info(response.getId());
                    } catch (Exception ex) {
                        LOG.warn(ex.getMessage());
                    }
                }
            }
        }

        // Main code logic

        // Close things
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";

        Properties properties = PropertiesHelper.getInstance().getProperties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient() {
        String url = "http://localhost:9200";
        URI uri = URI.create(url);
        String host = uri.getHost();
        int port = uri.getPort();
        String userInfo = uri.getUserInfo();
        RestClientBuilder restClientBuilder;

        if (userInfo == null) {
            restClientBuilder = RestClient.builder(new HttpHost(host, port, "http"));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restClientBuilder = RestClient.builder(new HttpHost(host, port, uri.getScheme()))
                    .setHttpClientConfigCallback(asyncClientBuilder -> asyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy()));
        }

        return new RestHighLevelClient(restClientBuilder);
    }

    private static String extractId(final String jsonValue) {
        return JsonParser.parseString(jsonValue)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}