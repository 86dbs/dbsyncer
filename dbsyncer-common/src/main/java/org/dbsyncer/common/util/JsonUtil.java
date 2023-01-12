package org.dbsyncer.common.util;

import com.alibaba.fastjson2.JSON;

import java.util.List;
import java.util.Map;

public abstract class JsonUtil {

    public static String objToJson(Object obj) {
        return JSON.toJSONString(obj);
    }

    public static <T> T jsonToObj(String json, Class<T> valueType) {
        return JSON.parseObject(json, valueType);
    }

    public static <T> List<T> jsonToArray(String json, Class<T> valueType) {
        return JSON.parseArray(json, valueType);
    }

    public static Map parseMap(Object obj) {
        return parseMap(objToJson(obj));
    }

    public static Map parseMap(String json) {
        return JSON.parseObject(json);
    }

    public static List parseList(String json) {
        return JSON.parseArray(json).toList(Map.class);
    }
}