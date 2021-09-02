package org.dbsyncer.connector.util;

import org.dbsyncer.common.util.StringUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.ESConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public abstract class ESUtil {

    public static final String PROPERTIES = "properties";
    private static final int ADDRESS_LENGTH = 2;

    private ESUtil() {
    }

    public static RestHighLevelClient getConnection(ESConfig config) {
        String[] ipAddress = StringUtil.split(config.getUrl(), ",");
        HttpHost[] hosts = Arrays.stream(ipAddress).map(node -> makeHttpHost(node, config.getSchema())).filter(Objects::nonNull).toArray(
                HttpHost[]::new);
        RestClientBuilder builder = RestClient.builder(hosts);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(config.getUsername(), config.getPassword());
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
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

    /**
     * 根据配置创建HttpHost
     *
     * @param address
     * @param scheme
     * @return
     */
    private static HttpHost makeHttpHost(String address, String scheme) {
        String[] arr = address.split(":");
        if (arr.length == ADDRESS_LENGTH) {
            String ip = arr[0];
            int port = Integer.parseInt(arr[1]);
            return new HttpHost(ip, port, scheme);
        } else {
            return null;
        }
    }
}