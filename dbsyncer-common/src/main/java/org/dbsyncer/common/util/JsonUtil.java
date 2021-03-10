package org.dbsyncer.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public abstract class JsonUtil {

    public static String objToJson(Object obj) {
        return JSON.toJSONString(obj, SerializerFeature.DisableCircularReferenceDetect);
    }

    public static <T> T jsonToObj(String json, Class<T> valueType) {
        return JSON.parseObject(json, valueType);
    }

}