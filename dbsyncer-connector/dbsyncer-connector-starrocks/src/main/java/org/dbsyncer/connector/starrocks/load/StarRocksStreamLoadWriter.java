/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks.load;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.starrocks.StarRocksException;
import org.dbsyncer.connector.starrocks.constant.StarRocksConstant;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.plugin.PluginContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * StarRocks Stream Load 写入实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public final class StarRocksStreamLoadWriter {

    private static final int CONNECT_TIMEOUT_MS = 10000;

    private static final int READ_TIMEOUT_MS = 600000;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public Result write(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            throw new StarRocksException("writer data can not be empty.");
        }

        DatabaseConfig config = connectorInstance.getConfig();
        String database = resolveDatabase(connectorInstance, config);
        String table = context.getTargetTable().getName();
        String body = buildJsonLines(data);
        String label = buildLabel(context.getTraceId());
        String loadUrl = buildStreamLoadUrl(config, database, table);

        Result result = new Result();
        try {
            String response = executeStreamLoad(config, loadUrl, label, body);
            parseResponse(response, data, result);
        } catch (Exception e) {
            result.addFailData(data);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    private String resolveDatabase(DatabaseConnectorInstance connectorInstance, DatabaseConfig config) {
        String database = connectorInstance.getCatalog();
        if (StringUtil.isBlank(database)) {
            database = config.getDatabase();
        }
        if (StringUtil.isBlank(database)) {
            throw new StarRocksException("Stream Load 需要指定数据库名");
        }
        return database;
    }

    private String buildJsonLines(List<Map> data) {
        StringBuilder builder = new StringBuilder();
        for (Map row : data) {
            builder.append(JsonUtil.objToJsonSafe(row)).append('\n');
        }
        return builder.toString();
    }

    private String buildLabel(String traceId) {
        String prefix = StringUtil.isBlank(traceId) ? "dbsyncer" : traceId.replaceAll("[^a-zA-Z0-9_\\-]", "_");
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private String buildStreamLoadUrl(DatabaseConfig config, String database, String table) {
        return String.format("http://%s:%d/api/%s/%s/_stream_load",
                config.getHost(), StarRocksConstant.getHttpPort(config), database, table);
    }

    private String executeStreamLoad(DatabaseConfig config, String loadUrl, String label, String body) throws IOException {
        String currentUrl = loadUrl;
        for (int redirectCount = 0; redirectCount < 3; redirectCount++) {
            HttpURLConnection connection = openConnection(config, currentUrl, label);
            try {
                writeBody(connection, body);
                int statusCode = connection.getResponseCode();
                if (statusCode == HttpURLConnection.HTTP_MOVED_TEMP || statusCode == HttpURLConnection.HTTP_SEE_OTHER
                        || statusCode == 307 || statusCode == 308) {
                    String location = connection.getHeaderField("Location");
                    if (StringUtil.isBlank(location)) {
                        throw new StarRocksException("Stream Load 重定向缺少 Location 头");
                    }
                    currentUrl = location;
                    continue;
                }
                return readResponse(connection, statusCode);
            } finally {
                connection.disconnect();
            }
        }
        throw new StarRocksException("Stream Load 重定向次数过多");
    }

    private HttpURLConnection openConnection(DatabaseConfig config, String loadUrl, String label) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(loadUrl).openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
        connection.setReadTimeout(READ_TIMEOUT_MS);
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setRequestProperty("Authorization", basicAuth(config.getUsername(), config.getPassword()));
        connection.setRequestProperty("label", label);
        connection.setRequestProperty("format", "json");
        connection.setRequestProperty("read_json_by_line", "true");
        connection.setRequestProperty("Expect", "100-continue");
        connection.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        return connection;
    }

    private void writeBody(HttpURLConnection connection, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        connection.setRequestProperty("Content-Length", String.valueOf(payload.length));
        try (OutputStream outputStream = connection.getOutputStream()) {
            outputStream.write(payload);
            outputStream.flush();
        }
    }

    private String readResponse(HttpURLConnection connection, int statusCode) throws IOException {
        InputStream stream = statusCode >= HttpURLConnection.HTTP_BAD_REQUEST
                ? connection.getErrorStream() : connection.getInputStream();
        if (stream == null) {
            throw new StarRocksException("Stream Load 响应为空, status=" + statusCode);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            if (statusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
                throw new StarRocksException("Stream Load HTTP 失败, status=" + statusCode + ", body=" + response);
            }
            return response.toString();
        }
    }

    private void parseResponse(String response, List<Map> data, Result result) {
        Map responseMap = JsonUtil.jsonToObj(response, Map.class);
        if (responseMap == null) {
            result.addFailData(data);
            result.getError().append("Stream Load 响应解析失败: ").append(response).append(System.lineSeparator());
            return;
        }
        Object status = responseMap.get("Status");
        if (status == null) {
            status = responseMap.get("status");
        }
        String statusText = status == null ? "" : String.valueOf(status);
        if (StringUtil.equalsIgnoreCase(statusText, "Success")
                || StringUtil.equalsIgnoreCase(statusText, "Publish Timeout")) {
            result.addSuccessData(data);
            return;
        }
        Object message = responseMap.get("Message");
        if (message == null) {
            message = responseMap.get("message");
        }
        String errorMessage = message == null ? response : String.valueOf(message);
        result.addFailData(data);
        result.getError().append(errorMessage).append(System.lineSeparator());
    }

    private String basicAuth(String username, String password) {
        String token = username + ":" + (password == null ? "" : password);
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
