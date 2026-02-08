/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.serialization;

import org.dbsyncer.common.util.JsonUtil;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-12-16 23:09
 */
public class MapToJsonSerializer implements Serializer<Map> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get("serializer.encoding");
        }
        if (encodingValue instanceof String) {
            encoding = (String) encodingValue;
        }
    }

    @Override
    public byte[] serialize(String topic, Map data) {
        try {
            if (data == null) {
                return null;
            }
            return JsonUtil.objToJson(data).getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
