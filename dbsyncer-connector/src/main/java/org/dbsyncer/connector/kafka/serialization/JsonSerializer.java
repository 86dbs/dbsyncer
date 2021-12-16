package org.dbsyncer.connector.kafka.serialization;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/16 23:35
 */
public class JsonSerializer extends AbstractValueSerializer {
    public JsonSerializer() {
        super(StringDeserializer.class.getName(), StringSerializer.class.getName());
    }
}