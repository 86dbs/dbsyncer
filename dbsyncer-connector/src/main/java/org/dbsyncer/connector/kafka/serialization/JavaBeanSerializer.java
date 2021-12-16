package org.dbsyncer.connector.kafka.serialization;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/16 23:35
 */
public class JavaBeanSerializer extends AbstractValueSerializer {
    public JavaBeanSerializer() {
        super(ObjectValueDeserializer.class.getName(), ObjectValueSerializer.class.getName());
    }
}