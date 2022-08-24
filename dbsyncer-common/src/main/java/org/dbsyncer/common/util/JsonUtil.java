package org.dbsyncer.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.List;

public abstract class JsonUtil {

    public static String objToJson(Object obj) {
        return JSON.toJSONString(obj, SerializerFeature.DisableCircularReferenceDetect);
    }

    public static <T> T jsonToObj(String json, Class<T> valueType) {
        return JSON.parseObject(json, valueType);
    }

    public static <T> List<T> jsonToArray(String json, Class<T> valueType) {
        return JSON.parseArray(json, valueType);
    }

    public static JSONObject parseObject(String json) {
        return JSON.parseObject(json);
    }

    public static JSONArray parseArray(String json) {
        return JSON.parseArray(json);
    }
}