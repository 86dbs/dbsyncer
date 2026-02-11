/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.dbsyncer.connector.http.config.HttpConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

/**
 * Http连接器实例
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:01
 */
public final class HttpConnectorInstance implements ConnectorInstance<HttpConfig, CloseableHttpClient> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 连接池管理器
     */
    private PoolingHttpClientConnectionManager connectionManager;

    /**
     * Http 客户端
     */
    private CloseableHttpClient httpClient;

    /**
     * 默认连接超时时间（毫秒）
     */
    private final int DEFAULT_CONNECTION_TIMEOUT = 30000;

    /**
     * 默认读取超时时间（毫秒）
     */
    private final int DEFAULT_SO_TIMEOUT = 30000;

    /**
     * 默认每个路由最大连接数
     */
    private final int DEFAULT_MAX_PER_ROUTE = 50;

    /**
     * 默认最大总连接数
     */
    private final int DEFAULT_MAX_TOTAL = 500;

    private HttpConfig config;

    public HttpConnectorInstance(HttpConfig config) {
        this.config = config;
        try {
            initHttpClient();
        } catch (Exception e) {
            throw new HttpException(e);
        }
    }

    /**
     * 初始化 HttpClient
     */
    private void initHttpClient() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        if (httpClient == null) {
            logger.info("Initializing HttpClient ...");

            // 创建连接池管理器
            // 1. 创建 SSL 上下文和 Socket 工厂
            SSLContextBuilder builder = new SSLContextBuilder();
            // 信任所有证书
            builder.loadTrustMaterial(null, (TrustStrategy) (x509Certificates, s) -> true);
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(builder.build(), NoopHostnameVerifier.INSTANCE);

            // 2. 注册 HTTP 和 HTTPS 协议处理器
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", new PlainConnectionSocketFactory())
                    .register("https", sslConnectionSocketFactory)
                    .build();
            connectionManager = new PoolingHttpClientConnectionManager(registry);

            // 配置连接池
            connectionManager.setMaxTotal(DEFAULT_MAX_TOTAL);
            connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);

            // 配置连接参数
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                    .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
                    .setSocketTimeout(DEFAULT_SO_TIMEOUT)
                    .setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT);

            // 构建 HttpClient
            HttpClientBuilder httpClientBuilder = HttpClients.custom()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(requestConfigBuilder.build());

            httpClient = httpClientBuilder.build();
            logger.info("HttpClient initialized successfully");
        }
    }

    @Override
    public String getServiceUrl() {
        return config.getUrl();
    }

    @Override
    public HttpConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(HttpConfig config) {
        this.config = config;
    }

    @Override
    public CloseableHttpClient getConnection() {
        return httpClient;
    }

    /**
     * 关闭 HttpClient
     */
    @Override
    public void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
                logger.info("HttpClient closed successfully");
            }
        } catch (Exception e) {
            logger.error("Failed to close HttpClient", e);
        }
        try {
            if (connectionManager != null) {
                connectionManager.close();
                logger.info("ConnectionManager closed successfully");
            }
        } catch (Exception e) {
            logger.error("Failed to close ConnectionManager", e);
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}