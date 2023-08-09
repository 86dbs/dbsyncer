package org.dbsyncer.connector.util;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.ESConfig;
import org.dbsyncer.connector.es.EasyRestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public abstract class ESUtil {

    public static final String PROPERTIES = "properties";

    private ESUtil() {
    }

    public static EasyRestHighLevelClient getConnection(ESConfig config) {
        String[] ipAddress = StringUtil.split(config.getUrl(), ",");
        HttpHost[] hosts = Arrays.stream(ipAddress).map(node -> HttpHost.create(node)).filter(Objects::nonNull).toArray(
                HttpHost[]::new);
        RestClientBuilder builder = RestClient.builder(hosts);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(new TrustAllStrategy()).build();
            SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext, NoopHostnameVerifier.INSTANCE);
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLStrategy(sessionStrategy));
            final EasyRestHighLevelClient client = new EasyRestHighLevelClient(builder);
            client.ping(RequestOptions.DEFAULT);
            return client;
        } catch (Exception e) {
            throw new ConnectorException(String.format("Failed to connect to ElasticSearch on %s, %s", config.getUrl(), e.getMessage()));
        }
    }

    public static void close(RestHighLevelClient client) {
        if (null != client) {
            try {
                client.close();
            } catch (IOException e) {
                throw new ConnectorException(e.getMessage());
            }
        }
    }

}