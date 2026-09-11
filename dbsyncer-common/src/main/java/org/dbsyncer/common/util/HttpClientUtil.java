/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * 基于 {@link HttpURLConnection} 的简易 HTTP 客户端（节点内网互调）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-11
 */
public abstract class HttpClientUtil {

    private HttpClientUtil() {
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
        return exchange("GET", url, null, null, connectTimeoutMs, readTimeoutMs);
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
        return exchange("POST", url, formBody, "application/x-www-form-urlencoded; charset=UTF-8",
                connectTimeoutMs, readTimeoutMs);
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

    private static HttpResult exchange(String method, String url, String body, String contentType,
                                       int connectTimeoutMs, int readTimeoutMs) throws Exception {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            NetUtil.applyInsecureSslIfNeeded(connection);
            connection.setRequestMethod(method);
            connection.setConnectTimeout(connectTimeoutMs);
            connection.setReadTimeout(readTimeoutMs);
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

    /**
     * HTTP 响应结果。
     */
    public static final class HttpResult {

        private final int statusCode;
        private final String body;

        public HttpResult(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body == null ? StringUtil.EMPTY : body;
        }

        /**
         * @return HTTP 状态码是否为 200
         */
        public boolean isOk() {
            return statusCode == 200;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getBody() {
            return body;
        }
    }
}
