/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.util;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mongodb.MongoDBException;
import org.dbsyncer.connector.mongodb.config.MongoDBConfig;
import org.dbsyncer.connector.mongodb.constant.MongoDBConstant;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public abstract class MongoUtil {

    private MongoUtil() {
    }

    public static MongoClient createClient(MongoDBConfig config) {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(buildConnectionString(config)))
                .applyToConnectionPoolSettings(builder -> builder.maxSize(config.getMaxPoolSize()))
                .applyToSocketSettings(builder -> builder.connectTimeout(10, TimeUnit.SECONDS)
                        .readTimeout(60, TimeUnit.SECONDS))
                .build();
        return MongoClients.create(settings);
    }

    public static String buildConnectionString(MongoDBConfig config) {
        StringBuilder uri = new StringBuilder("mongodb://");
        if (StringUtil.isNotBlank(config.getUsername())) {
            uri.append(encode(config.getUsername())).append(':').append(encode(config.getPassword())).append('@');
        }
        uri.append(config.getHost()).append(':').append(config.getPort());
        if (StringUtil.isNotBlank(config.getDatabase())) {
            uri.append('/').append(config.getDatabase());
        }
        StringBuilder params = new StringBuilder();
        if (StringUtil.isNotBlank(config.getAuthDatabase())) {
            appendParam(params, "authSource", config.getAuthDatabase());
        }
        if (config.getProperties() != null) {
            config.getProperties().forEach((k, v) -> appendParam(params, String.valueOf(k), String.valueOf(v)));
        }
        if (params.length() > 0) {
            uri.append('?').append(params);
        }
        return uri.toString();
    }

    public static void close(MongoClient client) {
        if (client != null) {
            client.close();
        }
    }

    public static Map<String, Object> toMap(Document document) {
        Map<String, Object> row = new LinkedHashMap<>();
        if (document == null) {
            return row;
        }
        document.forEach((key, value) -> row.put(key, normalizeReadValue(value)));
        return row;
    }

    public static Document toDocument(Map<?, ?> row) {
        Document document = new Document();
        if (row == null) {
            return document;
        }
        row.forEach((key, value) -> {
            if (key == null) {
                return;
            }
            document.append(String.valueOf(key), normalizeWriteValue(String.valueOf(key), value));
        });
        return document;
    }

    public static Object normalizeWriteValue(String fieldName, Object value) {
        if (value == null) {
            return null;
        }
        if (MongoDBConstant.ID_FIELD.equals(fieldName)) {
            return normalizeId(value);
        }
        if (value instanceof Map) {
            return toDocument((Map<?, ?>) value);
        }
        if (value instanceof List) {
            List<Object> list = new ArrayList<>();
            for (Object item : (List<?>) value) {
                list.add(item instanceof Map ? toDocument((Map<?, ?>) item) : item);
            }
            return list;
        }
        return value;
    }

    public static Object normalizeId(Object value) {
        if (value instanceof ObjectId) {
            return value;
        }
        if (value instanceof String && ObjectId.isValid((String) value)) {
            return new ObjectId((String) value);
        }
        return value;
    }

    public static Object normalizeReadValue(Object value) {
        if (value instanceof ObjectId) {
            return value.toString();
        }
        if (value instanceof Decimal128) {
            return ((Decimal128) value).bigDecimalValue();
        }
        if (value instanceof Document) {
            return toMap((Document) value);
        }
        if (value instanceof List) {
            List<Object> list = new ArrayList<>();
            for (Object item : (List<?>) value) {
                list.add(normalizeReadValue(item));
            }
            return list;
        }
        if (value instanceof Date) {
            return new java.sql.Timestamp(((Date) value).getTime());
        }
        return value;
    }

    public static String inferTypeName(String fieldName, Object value) {
        if (MongoDBConstant.ID_FIELD.equals(fieldName)) {
            return "objectId";
        }
        if (value == null) {
            return "string";
        }
        if (value instanceof Integer) {
            return "int";
        }
        if (value instanceof Long) {
            return "long";
        }
        if (value instanceof Double || value instanceof Float) {
            return "double";
        }
        if (value instanceof Boolean) {
            return "bool";
        }
        if (value instanceof Decimal128) {
            return "decimal";
        }
        if (value instanceof Date) {
            return "date";
        }
        if (value instanceof ObjectId) {
            return "objectId";
        }
        if (value instanceof List) {
            return "array";
        }
        if (value instanceof Document || value instanceof Map) {
            return "object";
        }
        return "string";
    }

    private static void appendParam(StringBuilder params, String key, String value) {
        if (StringUtil.isBlank(key) || value == null) {
            return;
        }
        if (params.length() > 0) {
            params.append('&');
        }
        params.append(encode(key)).append('=').append(encode(value));
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            throw new MongoDBException(e);
        }
    }
}
