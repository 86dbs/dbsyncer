package org.dbsyncer.connector.kafka.serialization;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/16 23:34
 */
public abstract class AbstractValueSerializer {
    protected String deserializer;

    protected String serializer;

    public AbstractValueSerializer(String deserializer, String serializer) {
        this.deserializer = deserializer;
        this.serializer = serializer;
    }

    public String getDeserializer() {
        return deserializer;
    }

    public String getSerializer() {
        return serializer;
    }

}