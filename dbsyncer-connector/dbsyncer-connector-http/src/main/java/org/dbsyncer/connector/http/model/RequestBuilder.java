/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.model;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.http.enums.ContentTypeEnum;
import org.dbsyncer.connector.http.enums.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Http连接器配置构建器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-11 00:01
 */
public final class RequestBuilder {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 客户端
     */
    private final CloseableHttpClient client;

    /**
     * 请求URL
     */
    private String url;

    /**
     * 请求方法
     */
    private HttpMethod method;

    /**
     * 查询参数
     */
    private Map<String, String> queryParams = new HashMap<>();

    /**
     * 请求体参数（用于表单提交）
     */
    private Map<String, String> bodyParams = new HashMap<>();

    /**
     * 请求体（JSON字符串）
     */
    private String jsonBody;

    /**
     * 请求头
     */
    private Map<String, String> headers = new HashMap<>();

    /**
     * Content-Type
     */
    private ContentTypeEnum contentType;

    /**
     * 连接超时时间（毫秒）
     */
    private Integer connectionTimeout = 30000;

    /**
     * 读取超时时间（毫秒）
     */
    private Integer socketTimeout = 30000;

    /**
     * 从连接池获取连接的超时时间（毫秒）
     */
    private Integer connectionRequestTimeout = 30000;

    /**
     * 字符编码
     */
    private String charset = "UTF-8";

    /**
     * 重试次数
     */
    private int retryTimes = 0;

    /**
     * 构造函数
     *
     * @param client      客户端
     * @param url         请求URL
     * @param method      请求方法
     */
    public RequestBuilder(CloseableHttpClient client, String url, HttpMethod method) {
        this.client = client;
        if (StringUtil.isBlank(url)) {
            throw new IllegalArgumentException("URL cannot be null or empty");
        }
        this.url = url;
        this.method = method;
    }

    /**
     * 添加查询参数
     *
     * @param key   参数名
     * @param value 参数值
     * @return 当前构建器
     */
    public RequestBuilder addParam(String key, Object value) {
        if (key != null && value != null) {
            queryParams.put(key, String.valueOf(value));
        }
        return this;
    }

    /**
     * 添加多个查询参数
     *
     * @param params 参数Map
     * @return 当前构建器
     */
    public RequestBuilder addParams(Map<String, Object> params) {
        if (params != null) {
            params.forEach((key, value) -> addParam(key, value));
        }
        return this;
    }

    /**
     * 添加请求体参数（用于 application/x-www-form-urlencoded）
     *
     * @param key   参数名
     * @param value 参数值
     * @return 当前构建器
     */
    public RequestBuilder addBodyParam(String key, Object value) {
        if (key != null && value != null) {
            bodyParams.put(key, String.valueOf(value));
        }
        return this;
    }

    /**
     * 添加多个请求体参数
     *
     * @param params 参数Map
     * @return 当前构建器
     */
    public RequestBuilder addBodyParams(Map<String, Object> params) {
        if (params != null) {
            params.forEach((key, value) -> addBodyParam(key, value));
        }
        return this;
    }

    /**
     * 设置请求体为JSON
     *
     * @param body 请求体对象
     * @return 当前构建器
     */
    public RequestBuilder setBodyAsJson(Object body) {
        if (body != null) {
            this.jsonBody = JsonUtil.objToJson(body);
            this.contentType = ContentTypeEnum.APPLICATION_JSON;
        }
        return this;
    }

    /**
     * 设置请求体为JSON字符串
     *
     * @param jsonString JSON字符串
     * @return 当前构建器
     */
    public RequestBuilder setBodyAsJsonString(String jsonString) {
        if (StringUtil.isNotBlank(jsonString)) {
            this.jsonBody = jsonString;
            this.contentType = ContentTypeEnum.APPLICATION_JSON;
        }
        return this;
    }

    /**
     * 添加请求头
     *
     * @param key   头名称
     * @param value 头值
     * @return 当前构建器
     */
    public RequestBuilder addHeader(String key, Object value) {
        if (key != null && value != null) {
            headers.put(key, String.valueOf(value));
        }
        return this;
    }

    /**
     * 添加多个请求头
     *
     * @param headers 请求头Map
     * @return 当前构建器
     */
    public RequestBuilder addHeaders(Map<String, String> headers) {
        if (headers != null) {
            headers.forEach((key, value) -> addHeader(key, value));
        }
        return this;
    }

    /**
     * 设置 Content-Type
     *
     * @param contentType Content-Type
     * @return 当前构建器
     */
    public RequestBuilder setContentType(ContentTypeEnum contentType) {
        if (contentType != null) {
            this.contentType = contentType;
        }
        return this;
    }

    /**
     * 设置字符编码
     *
     * @param charset 字符编码
     * @return 当前构建器
     */
    public RequestBuilder setCharset(String charset) {
        if (StringUtil.isNotBlank(charset)) {
            this.charset = charset;
        }
        return this;
    }

    /**
     * 设置连接超时时间（毫秒）
     *
     * @param connectionTimeout 连接超时时间
     * @return 当前构建器
     */
    public RequestBuilder setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    /**
     * 设置读取超时时间（毫秒）
     *
     * @param socketTimeout 读取超时时间
     * @return 当前构建器
     */
    public RequestBuilder setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    /**
     * 设置从连接池获取连接的超时时间（毫秒）
     *
     * @param connectionRequestTimeout 连接请求超时时间
     * @return 当前构建器
     */
    public RequestBuilder setConnectionRequestTimeout(int connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
        return this;
    }

    /**
     * 设置重试次数
     *
     * @param retryTimes 重试次数
     * @return 当前构建器
     */
    public RequestBuilder setRetryTimes(int retryTimes) {
        this.retryTimes = Math.max(0, retryTimes);
        return this;
    }

    /**
     * 执行请求
     *
     * @return HTTP响应
     */
    public HttpResponse<String> execute() {
        return execute(String.class);
    }

    /**
     * 执行请求并返回指定类型的响应
     *
     * @param clazz 响应类型
     * @param <T>   泛型类型
     * @return HTTP响应
     */
    public <T> HttpResponse<T> execute(Class<T> clazz) {
        long startTime = System.currentTimeMillis();
        HttpResponse<T> response = new HttpResponse<>();

        try {
            // 构建URL
            String requestUrl = buildUrl();

            // 创建请求配置
            RequestConfig requestConfig = createRequestConfig();

            // 根据方法类型创建请求
            HttpRequestBase httpRequest = createHttpRequest(requestUrl, requestConfig);

            // 设置请求头
            setRequestHeaders(httpRequest);

            // 设置请求体（对于 POST/PUT）
            if (httpRequest instanceof HttpEntityEnclosingRequestBase) {
                setRequestBody((HttpEntityEnclosingRequestBase) httpRequest);
            }

            // 执行请求（带重试）
            CloseableHttpResponse httpResponse = executeWithRetry(httpRequest);

            // 处理响应
            response = handleResponse(httpResponse, clazz);

        } catch (Exception e) {
            logger.error("HTTP request failed: {} {}", method, url, e);
            response.setSuccess(false);
            response.setMessage("HTTP request failed: " + e.getMessage());
        }

        response.setCostTime(System.currentTimeMillis() - startTime);

        // 记录请求耗时
        if (response.isSuccess()) {
            logger.debug("Request completed successfully: {} {}, status: {}, cost: {}ms",
                    method, url, response.getStatusCode(), response.getCostTime());
        } else {
            logger.warn("Request failed: {} {}, status: {}, cost: {}ms, message: {}",
                    method, url, response.getStatusCode(), response.getCostTime(), response.getMessage());
        }

        return response;
    }

    /**
     * 构建完整URL（包含查询参数）
     *
     * @return 完整URL
     */
    private String buildUrl() {
        if (queryParams.isEmpty()) {
            return url;
        }

        StringBuilder sb = new StringBuilder(url);
        if (!url.contains("?")) {
            sb.append("?");
        } else {
            sb.append("&");
        }

        return buildQueryString(sb, queryParams);
    }

    /**
     * 构建查询字符串
     *
     * @param sb     StringBuilder
     * @param params 参数Map
     * @return 查询字符串
     */
    private String buildQueryString(StringBuilder sb, Map<String, String> params) {
        try {
            boolean first = true;
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (!first) {
                    sb.append("&");
                }
                sb.append(URLEncoder.encode(entry.getKey(), charset));
                sb.append("=");
                sb.append(URLEncoder.encode(entry.getValue(), charset));
                first = false;
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("Encoding error", e);
        }
        return sb.toString();
    }

    /**
     * 创建请求配置
     *
     * @return RequestConfig
     */
    private RequestConfig createRequestConfig() {
        RequestConfig.Builder builder = RequestConfig.custom();
        builder.setConnectTimeout(connectionTimeout);
        builder.setSocketTimeout(socketTimeout);
        builder.setConnectionRequestTimeout(connectionRequestTimeout);
        return builder.build();
    }

    /**
     * 创建 HTTP 请求对象
     *
     * @param requestUrl    请求URL
     * @param requestConfig 请求配置
     * @return HttpRequestBase
     */
    private HttpRequestBase createHttpRequest(String requestUrl, RequestConfig requestConfig) {
        switch (method) {
            case GET:
                HttpGet httpGet = new HttpGet(requestUrl);
                httpGet.setConfig(requestConfig);
                return httpGet;
            case POST:
                HttpPost httpPost = new HttpPost(requestUrl);
                httpPost.setConfig(requestConfig);
                return httpPost;
            case PUT:
                HttpPut httpPut = new HttpPut(requestUrl);
                httpPut.setConfig(requestConfig);
                return httpPut;
            case DELETE:
                HttpDelete httpDelete = new HttpDelete(requestUrl);
                httpDelete.setConfig(requestConfig);
                return httpDelete;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        }
    }

    /**
     * 设置请求头
     *
     * @param request HTTP请求
     */
    private void setRequestHeaders(HttpRequestBase request) {
        // 设置默认请求头
        if (!headers.containsKey("User-Agent")) {
            headers.put("User-Agent", "HttpClientUtil/4.5.13");
        }
        if (!headers.containsKey("Accept")) {
            headers.put("Accept", "*/*");
        }

        // 添加所有请求头
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 设置请求体
     *
     * @param request HTTP请求
     * @throws UnsupportedEncodingException 不支持的编码异常
     */
    private void setRequestBody(HttpEntityEnclosingRequestBase request) throws IOException {
        HttpEntity entity = null;

        // 如果有 JSON body，优先使用 JSON
        if (StringUtil.isNotBlank(jsonBody)) {
            entity = new StringEntity(jsonBody, jsonContentType());
            request.setHeader("Content-Type", jsonContentType());
        }
        // 如果设置了 bodyParams，使用表单提交
        else if (!bodyParams.isEmpty()) {
            List<NameValuePair> params = new ArrayList<>();
            for (Map.Entry<String, String> entry : bodyParams.entrySet()) {
                params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            entity = new UrlEncodedFormEntity(params, charset);
        }

        if (entity != null) {
            request.setEntity(entity);
        }
    }

    /**
     * 获取 JSON Content-Type
     *
     * @return Content-Type
     */
    private String jsonContentType() {
        return ContentTypeEnum.APPLICATION_JSON.getCode() + "; charset=" + charset;
    }

    /**
     * 带重试的执行
     *
     * @param request HTTP请求
     * @return CloseableHttpResponse
     * @throws Exception 异常
     */
    private CloseableHttpResponse executeWithRetry(HttpRequestBase request) throws Exception {
        CloseableHttpResponse httpResponse = null;
        int actualRetryTimes = 0;
        IOException lastException = null;

        while (actualRetryTimes <= retryTimes) {
            try {
                httpResponse = client.execute(request);
                break;
            } catch (SocketTimeoutException e) {
                lastException = e;
                actualRetryTimes++;
                if (actualRetryTimes > retryTimes) {
                    break;
                }
                logger.warn("Socket timeout, retrying ({}/{})...", actualRetryTimes, retryTimes);
                Thread.sleep(100); // 重试前等待100毫秒
            }
        }

        if (lastException != null && actualRetryTimes > retryTimes) {
            throw lastException;
        }

        return httpResponse;
    }

    /**
     * 处理响应
     *
     * @param httpResponse HTTP响应
     * @param clazz        返回类型
     * @param <T>          泛型类型
     * @return HttpResponse
     * @throws IOException IO异常
     */
    private <T> HttpResponse<T> handleResponse(CloseableHttpResponse httpResponse, Class<T> clazz) throws IOException {
        try {
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            HttpEntity entity = httpResponse.getEntity();
            String body = entity != null ? EntityUtils.toString(entity, charset) : null;

            HttpResponse<T> response = new HttpResponse<>(statusCode, body);
            response.setStatusCode(statusCode);
            response.setBody(body);
            response.setSuccess(statusCode >= 200 && statusCode < 300);

            // 消耗实体内容，确保连接可以释放
            EntityUtils.consumeQuietly(entity);

            return response;
        } finally {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
    }
}
