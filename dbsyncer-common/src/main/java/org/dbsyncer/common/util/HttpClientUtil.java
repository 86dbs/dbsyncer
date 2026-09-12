/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.dbsyncer.common.model.HttpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Apache HttpClient 的简易 HTTP 工具，仅封装 GET / POST。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-11
 */
public abstract class HttpClientUtil {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientUtil.class);

    /**
     * 节点间内部控制面共享密钥请求头。
     */
    public static final String CLUSTER_TOKEN_HEADER = "X-Cluster-Token";

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_READ_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_MAX_TOTAL = 200;
    private static final int DEFAULT_MAX_PER_ROUTE = 50;
    private static final int DEFAULT_RETRY_COUNT = 3;

    private static final CloseableHttpClient CLIENT = createClient();

    private HttpClientUtil() {
    }

    /**
     * 构建节点间调用头。token 为空时返回空 Map。
     */
    public static Map<String, String> clusterTokenHeaders(String token) {
        if (StringUtil.isBlank(token)) {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new LinkedHashMap<>(2);
        headers.put(CLUSTER_TOKEN_HEADER, token);
        return Collections.unmodifiableMap(headers);
    }

    public static HttpResult get(String url) throws Exception {
        return get(url, null, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    public static HttpResult get(String url, int connectTimeoutMs, int readTimeoutMs) throws Exception {
        return get(url, null, connectTimeoutMs, readTimeoutMs);
    }

    public static HttpResult get(String url, Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs)
            throws Exception {
        HttpGet request = new HttpGet(url);
        applyConfig(request, headers, connectTimeoutMs, readTimeoutMs);
        return execute(request);
    }

    public static HttpResult post(String url, String body, ContentType contentType) throws Exception {
        return post(url, body, contentType, null, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    public static HttpResult post(String url, String body, ContentType contentType, Map<String, String> headers,
                                  int connectTimeoutMs, int readTimeoutMs) throws Exception {
        HttpPost request = new HttpPost(url);
        applyConfig(request, headers, connectTimeoutMs, readTimeoutMs);
        if (body != null) {
            request.setEntity(new StringEntity(body, contentType == null
                    ? ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8)
                    : contentType));
        }
        return execute(request);
    }

    private static void applyConfig(HttpRequestBase request, Map<String, String> headers,
                                    int connectTimeoutMs, int readTimeoutMs) {
        request.setConfig(RequestConfig.custom()
                .setConnectTimeout(Math.max(connectTimeoutMs, 0))
                .setSocketTimeout(Math.max(readTimeoutMs, 0))
                .setConnectionRequestTimeout(Math.max(connectTimeoutMs, 0))
                .build());
        if (headers == null || headers.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey() != null && header.getValue() != null) {
                request.setHeader(header.getKey(), header.getValue());
            }
        }
    }

    private static HttpResult execute(HttpUriRequest request) throws Exception {
        try (CloseableHttpResponse response = CLIENT.execute(request)) {
            int status = response.getStatusLine().getStatusCode();
            String body = response.getEntity() == null
                    ? StringUtil.EMPTY
                    : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            return new HttpResult(status, body);
        }
    }

    private static CloseableHttpClient createClient() {
        try {
            TrustStrategy trustAll = (chain, authType) -> true;
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, trustAll).build();
            SSLConnectionSocketFactory sslSocketFactory =
                    new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);

            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", sslSocketFactory)
                    .build();

            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
            connectionManager.setMaxTotal(DEFAULT_MAX_TOTAL);
            connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
            connectionManager.setValidateAfterInactivity(5_000);

            RequestConfig defaultConfig = RequestConfig.custom()
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MS)
                    .setSocketTimeout(DEFAULT_READ_TIMEOUT_MS)
                    .setConnectionRequestTimeout(DEFAULT_CONNECT_TIMEOUT_MS)
                    .build();

            CloseableHttpClient client = HttpClients.custom()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(defaultConfig)
                    // IO 异常重试；requestSentRetryEnabled=true 允许可重复 POST 重试
                    .setRetryHandler(new DefaultHttpRequestRetryHandler(DEFAULT_RETRY_COUNT, true))
                    .evictExpiredConnections()
                    .evictIdleConnections(30, TimeUnit.SECONDS)
                    .build();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> closeQuietly(client), "http-client-shutdown"));
            return client;
        } catch (Exception e) {
            throw new IllegalStateException("初始化 HttpClient 失败", e);
        }
    }

    private static void closeQuietly(CloseableHttpClient client) {
        try {
            client.close();
        } catch (Exception e) {
            logger.warn("关闭 HttpClient 失败: {}", e.getMessage());
        }
    }
}
