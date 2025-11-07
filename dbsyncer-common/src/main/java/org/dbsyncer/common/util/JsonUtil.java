package org.dbsyncer.common.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String objToJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("JSON序列化失败: " + e.getMessage(), e);
        }
    }

    public static <T> T jsonToObj(String json, Class<T> valueType) {
        try {
            return objectMapper.readValue(json, valueType);
        } catch (IOException e) {
            throw new RuntimeException("JSON反序列化失败: " + e.getMessage(), e);
        }
    }

    public static <T> List<T> jsonToArray(String json, Class<T> valueType) {
        try {
            CollectionType collectionType = objectMapper.getTypeFactory()
                    .constructCollectionType(ArrayList.class, valueType);
            return objectMapper.readValue(json, collectionType);
        } catch (IOException e) {
            throw new RuntimeException("JSON数组反序列化失败: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseMap(Object obj) {
        return parseMap(objToJson(obj));
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseMap(String json) {
        try {
            MapType mapType = objectMapper.getTypeFactory()
                    .constructMapType(HashMap.class, String.class, Object.class);
            return objectMapper.readValue(json, mapType);
        } catch (IOException e) {
            throw new RuntimeException("JSON Map反序列化失败: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> parseList(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
        } catch (IOException e) {
            throw new RuntimeException("JSON List反序列化失败: " + e.getMessage(), e);
        }
    }
}