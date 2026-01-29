/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.schema;

import org.dbsyncer.connector.kafka.KafkaException;
import org.dbsyncer.connector.kafka.schema.support.KafkaBooleanType;
import org.dbsyncer.connector.kafka.schema.support.KafkaByteType;
import org.dbsyncer.connector.kafka.schema.support.KafkaBytesType;
import org.dbsyncer.connector.kafka.schema.support.KafkaDateType;
import org.dbsyncer.connector.kafka.schema.support.KafkaDecimalType;
import org.dbsyncer.connector.kafka.schema.support.KafkaDoubleType;
import org.dbsyncer.connector.kafka.schema.support.KafkaFloatType;
import org.dbsyncer.connector.kafka.schema.support.KafkaIntType;
import org.dbsyncer.connector.kafka.schema.support.KafkaLongType;
import org.dbsyncer.connector.kafka.schema.support.KafkaShortType;
import org.dbsyncer.connector.kafka.schema.support.KafkaStringType;
import org.dbsyncer.connector.kafka.schema.support.KafkaTimeType;
import org.dbsyncer.connector.kafka.schema.support.KafkaTimestampType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:18
 */
public final class KafkaSchemaResolver extends AbstractSchemaResolver {
    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new KafkaStringType(),
                new KafkaIntType(),
                new KafkaShortType(),
                new KafkaLongType(),
                new KafkaDecimalType(),
                new KafkaFloatType(),
                new KafkaDoubleType(),
                new KafkaDateType(),
                new KafkaTimestampType(),
                new KafkaTimeType(),
                new KafkaBooleanType(),
                new KafkaBytesType(),
                new KafkaByteType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new KafkaException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}