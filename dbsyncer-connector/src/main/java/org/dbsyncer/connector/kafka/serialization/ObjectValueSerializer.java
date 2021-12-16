package org.dbsyncer.connector.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/16 23:09
 */
public class ObjectValueSerializer implements Serializer<Object> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}