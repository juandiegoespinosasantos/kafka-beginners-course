package org.example.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * @author juandiegoespinosasantos@gmail.com
 * @version Jun 16, 2023
 * @since 11
 */
public class OpenSearchConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        // Create OpenSearch client
        try (RestHighLevelClient openSearchClient = createOpenSearchClient()) {
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
        }

        // Create Kafka client

        // Main code logic

        // Close things
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
}