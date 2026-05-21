/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.util;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.elasticsearch.api.EasyRestHighLevelClient;
import org.dbsyncer.connector.elasticsearch.config.ESConfig;
import org.dbsyncer.sdk.enums.FilterEnum;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * ES连接器工具类
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-25 23:10
 */
public abstract class ESUtil {

    public static final String PROPERTIES = "properties";

    private ESUtil() {
    }

    public static EasyRestHighLevelClient getConnection(ESConfig config) {
        String[] ipAddress = StringUtil.split(config.getUrl(), StringUtil.COMMA);
        HttpHost[] hosts = Arrays.stream(ipAddress).map(node->HttpHost.create(node)).filter(Objects::nonNull).toArray(HttpHost[]::new);
        RestClientBuilder builder = RestClient.builder(hosts);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(new TrustAllStrategy()).build();
            SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext, NoopHostnameVerifier.INSTANCE);
            builder.setHttpClientConfigCallback(httpClientBuilder->httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLStrategy(sessionStrategy));
            final EasyRestHighLevelClient client = new EasyRestHighLevelClient(builder);
            client.ping(RequestOptions.DEFAULT);
            return client;
        } catch (Exception e) {
            throw new ElasticsearchException(String.format("Failed to connect to ElasticSearch on %s, %s", config.getUrl(), e.getMessage()));
        }
    }

    public static void close(RestHighLevelClient client) {
        if (null != client) {
            try {
                client.close();
            } catch (IOException e) {
                throw new ElasticsearchException(e.getMessage());
            }
        }
    }

    /**
     * 构建 range 查询。7.x 客户端的 {@code RangeQueryBuilder#gt()} 等会序列化为 from/to，
     * ES 8+ 已不支持该写法，需直接使用 gt/gte/lt/lte。
     */
    public static QueryBuilder buildRangeQuery(String field, FilterEnum filterEnum, String value) {
        String boundKey;
        switch (filterEnum) {
            case GT:
                boundKey = "gt";
                break;
            case LT:
                boundKey = "lt";
                break;
            case GT_AND_EQUAL:
                boundKey = "gte";
                break;
            case LT_AND_EQUAL:
                boundKey = "lte";
                break;
            default:
                throw new IllegalArgumentException("unsupported range filter: " + filterEnum);
        }
        Map<String, Object> bounds = new LinkedHashMap<>(1);
        bounds.put(boundKey, value);
        Map<String, Object> rangeBody = new LinkedHashMap<>(1);
        rangeBody.put(field, bounds);
        Map<String, Object> query = new LinkedHashMap<>(1);
        query.put("range", rangeBody);
        return QueryBuilders.wrapperQuery(JsonUtil.objToJson(query));
    }

}