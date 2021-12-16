package org.dbsyncer.connector.kafka.serialization;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/16 23:09
 */
public class ObjectValueDeserializer implements Deserializer<Object> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}