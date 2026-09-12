/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.model.HttpResult;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 基于 {@link HttpURLConnection} 的简易 HTTP 客户端。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-11
 */
public abstract class HttpClientUtil {

    /**
     * 节点间内部控制面共享密钥请求头。
     * 闭源 Cluster SPI 调用 {@code /cluster/internal/message} 与 {@code /cluster/metrics} 时必须携带。
     */
    public static final String CLUSTER_TOKEN_HEADER = "X-Cluster-Token";

    private HttpClientUtil() {
    }

    /**
     * 构建节点间调用头。token 为空时返回空 Map（请求仍会发出，但服务端会拒绝 internal/metrics）。
     *
     * @param token {@code dbsyncer.cluster.internal-token}
     * @return 不可变请求头
     */
    public static Map<String, String> clusterTokenHeaders(String token) {
        if (StringUtil.isBlank(token)) {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new LinkedHashMap<String, String>(2);
        headers.put(CLUSTER_TOKEN_HEADER, token);
        return Collections.unmodifiableMap(headers);
    }

    /**
     * GET 请求。
     *
     * @param url              完整 URL
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读超时毫秒
     * @return 响应（含状态码与正文）
     * @throws Exception 网络或 IO 异常
     */
    public static HttpResult get(String url, int connectTimeoutMs, int readTimeoutMs) throws Exception {
        return get(url, null, connectTimeoutMs, readTimeoutMs);
    }

    /**
     * GET 请求（可带自定义头，例如 {@link #CLUSTER_TOKEN_HEADER}）。
     *
     * @param url              完整 URL
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读超时毫秒
     * @return 响应（含状态码与正文）
     * @throws Exception 网络或 IO 异常
     */
    public static HttpResult get(String url, Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs)
            throws Exception {
        return exchange("GET", url, null, null, headers, connectTimeoutMs, readTimeoutMs);
    }

    /**
     * GET 请求（可重试）。
     *
     * @param url              完整 URL
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读超时毫秒
     * @param retryTimes       总尝试次数（含首次，小于 1 按 1）
     * @return 响应（含状态码与正文）
     * @throws Exception 全部尝试均抛异常时抛出最后一次异常
     */
    public static HttpResult get(String url, Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs,
                                 int retryTimes) throws Exception {
        return get(url, headers, connectTimeoutMs, readTimeoutMs, retryTimes, 0L);
    }

    /**
     * GET 请求（可重试，带间隔）。
     *
     * @param url              完整 URL
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读超时毫秒
     * @param retryTimes       总尝试次数（含首次，小于 1 按 1）
     * @param retryIntervalMs  重试间隔毫秒（小于等于 0 不休眠）
     * @return 响应（含状态码与正文）
     * @throws Exception 全部尝试均抛异常时抛出最后一次异常
     */
    public static HttpResult get(String url, Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs,
                                 int retryTimes, long retryIntervalMs) throws Exception {
        return exchangeWithRetry("GET", url, null, null, headers, connectTimeoutMs, readTimeoutMs, retryTimes,
                retryIntervalMs);
    }

    /**
     * POST {@code application/x-www-form-urlencoded}。
     *
     * @param url              完整 URL
     * @param formBody         表单正文（已编码的 key=value&...）
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @return 响应（含状态码与正文）
     * @throws Exception 网络或 IO 异常
     */
    public static HttpResult postForm(String url, String formBody, int connectTimeoutMs, int readTimeoutMs) throws Exception {
        return postForm(url, formBody, null, connectTimeoutMs, readTimeoutMs);
    }

    /**
     * POST {@code application/x-www-form-urlencoded}（可带自定义头）。
     * 闭源 Cluster SPI 调用 {@code /cluster/internal/message} 时应传入 {@link #clusterTokenHeaders(String)}。
     *
     * @param url              完整 URL
     * @param formBody         表单正文（已编码的 key=value&...）
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @return 响应（含状态码与正文）
     * @throws Exception 网络或 IO 异常
     */
    public static HttpResult postForm(String url, String formBody, Map<String, String> headers,
                                      int connectTimeoutMs, int readTimeoutMs) throws Exception {
        return exchange("POST", url, formBody, "application/x-www-form-urlencoded; charset=UTF-8",
                headers, connectTimeoutMs, readTimeoutMs);
    }

    /**
     * POST form（可重试）。
     *
     * @param url              完整 URL
     * @param formBody         表单正文
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @param retryTimes       总尝试次数（含首次，小于 1 按 1）
     * @return 响应
     * @throws Exception 全部尝试均抛异常时抛出最后一次异常
     */
    public static HttpResult postForm(String url, String formBody, Map<String, String> headers,
                                      int connectTimeoutMs, int readTimeoutMs, int retryTimes) throws Exception {
        return postForm(url, formBody, headers, connectTimeoutMs, readTimeoutMs, retryTimes, 0L);
    }

    /**
     * POST form（可重试，带间隔）。
     *
     * @param url              完整 URL
     * @param formBody         表单正文
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @param retryTimes       总尝试次数（含首次，小于 1 按 1）
     * @param retryIntervalMs  重试间隔毫秒（小于等于 0 不休眠）
     * @return 响应
     * @throws Exception 全部尝试均抛异常时抛出最后一次异常
     */
    public static HttpResult postForm(String url, String formBody, Map<String, String> headers,
                                      int connectTimeoutMs, int readTimeoutMs, int retryTimes, long retryIntervalMs)
            throws Exception {
        return exchangeWithRetry("POST", url, formBody, "application/x-www-form-urlencoded; charset=UTF-8",
                headers, connectTimeoutMs, readTimeoutMs, retryTimes, retryIntervalMs);
    }

    /**
     * POST {@code application/json}。
     *
     * @param url              完整 URL
     * @param jsonBody         JSON 正文
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @return 响应（含状态码与正文）
     * @throws Exception 网络或 IO 异常
     */
    public static HttpResult postJson(String url, String jsonBody, int connectTimeoutMs, int readTimeoutMs)
            throws Exception {
        return postJson(url, jsonBody, null, connectTimeoutMs, readTimeoutMs);
    }

    /**
     * POST {@code application/json}（可带自定义头，例如 {@link #CLUSTER_TOKEN_HEADER}）。
     *
     * @param url              完整 URL
     * @param jsonBody         JSON 正文
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @return 响应（含状态码与正文）
     * @throws Exception 网络或 IO 异常
     */
    public static HttpResult postJson(String url, String jsonBody, Map<String, String> headers,
                                      int connectTimeoutMs, int readTimeoutMs) throws Exception {
        return exchange("POST", url, jsonBody, "application/json; charset=UTF-8",
                headers, connectTimeoutMs, readTimeoutMs);
    }

    /**
     * POST JSON（可重试）。
     *
     * @param url              完整 URL
     * @param jsonBody         JSON 正文
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @param retryTimes       总尝试次数（含首次，小于 1 按 1）
     * @return 响应
     * @throws Exception 全部尝试均抛异常时抛出最后一次异常
     */
    public static HttpResult postJson(String url, String jsonBody, Map<String, String> headers,
                                      int connectTimeoutMs, int readTimeoutMs, int retryTimes) throws Exception {
        return postJson(url, jsonBody, headers, connectTimeoutMs, readTimeoutMs, retryTimes, 0L);
    }

    /**
     * POST JSON（可重试，带间隔）。
     *
     * @param url              完整 URL
     * @param jsonBody         JSON 正文
     * @param headers          额外请求头，可为 null
     * @param connectTimeoutMs 连接超时毫秒
     * @param readTimeoutMs    读取超时毫秒
     * @param retryTimes       总尝试次数（含首次，小于 1 按 1）
     * @param retryIntervalMs  重试间隔毫秒（小于等于 0 不休眠）
     * @return 响应
     * @throws Exception 全部尝试均抛异常时抛出最后一次异常
     */
    public static HttpResult postJson(String url, String jsonBody, Map<String, String> headers,
                                      int connectTimeoutMs, int readTimeoutMs, int retryTimes, long retryIntervalMs)
            throws Exception {
        return exchangeWithRetry("POST", url, jsonBody, "application/json; charset=UTF-8",
                headers, connectTimeoutMs, readTimeoutMs, retryTimes, retryIntervalMs);
    }

    /**
     * URL 编码表单字段值。
     *
     * @param value 原始值
     * @return 编码后字符串；异常时回退原值
     */
    public static String urlEncode(String value) {
        try {
            return URLEncoder.encode(StringUtil.getIfBlank(value, StringUtil.EMPTY), "UTF-8");
        } catch (Exception e) {
            return value;
        }
    }

    private static HttpResult exchangeWithRetry(String method, String url, String body, String contentType,
                                                Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs,
                                                int retryTimes, long retryIntervalMs) throws Exception {
        int times = Math.max(1, retryTimes);
        Exception lastError = null;
        HttpResult lastResult = null;
        for (int i = 0; i < times; i++) {
            try {
                lastResult = exchange(method, url, body, contentType, headers, connectTimeoutMs, readTimeoutMs);
                if (lastResult.isOk()) {
                    return lastResult;
                }
            } catch (Exception e) {
                lastError = e;
            }
            if (i < times - 1) {
                sleepQuietly(retryIntervalMs);
            }
        }
        if (lastResult != null) {
            return lastResult;
        }
        throw lastError == null ? new Exception("HTTP request failed") : lastError;
    }

    private static void sleepQuietly(long retryIntervalMs) {
        if (retryIntervalMs <= 0) {
            return;
        }
        try {
            Thread.sleep(retryIntervalMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static HttpResult exchange(String method, String url, String body, String contentType,
                                       Map<String, String> headers, int connectTimeoutMs, int readTimeoutMs)
            throws Exception {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            NetUtil.applyInsecureSslIfNeeded(connection);
            connection.setRequestMethod(method);
            connection.setConnectTimeout(connectTimeoutMs);
            connection.setReadTimeout(readTimeoutMs);
            if (headers != null && !headers.isEmpty()) {
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    if (header.getKey() != null && header.getValue() != null) {
                        connection.setRequestProperty(header.getKey(), header.getValue());
                    }
                }
            }
            if (body != null) {
                connection.setDoOutput(true);
                if (StringUtil.isNotBlank(contentType)) {
                    connection.setRequestProperty("Content-Type", contentType);
                }
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(bytes);
                }
            }
            int status = connection.getResponseCode();
            String responseBody = readBody(connection, status);
            return new HttpResult(status, responseBody);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static String readBody(HttpURLConnection connection, int status) throws Exception {
        InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
        if (stream == null) {
            stream = connection.getInputStream();
        }
        if (stream == null) {
            return StringUtil.EMPTY;
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

}
