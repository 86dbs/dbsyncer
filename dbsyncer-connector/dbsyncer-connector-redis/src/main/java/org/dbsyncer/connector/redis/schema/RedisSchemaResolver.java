/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.redis.schema;

import org.dbsyncer.connector.redis.RedisException;
import org.dbsyncer.connector.redis.schema.support.RedisBooleanType;
import org.dbsyncer.connector.redis.schema.support.RedisByteType;
import org.dbsyncer.connector.redis.schema.support.RedisBytesType;
import org.dbsyncer.connector.redis.schema.support.RedisDateType;
import org.dbsyncer.connector.redis.schema.support.RedisDecimalType;
import org.dbsyncer.connector.redis.schema.support.RedisDoubleType;
import org.dbsyncer.connector.redis.schema.support.RedisFloatType;
import org.dbsyncer.connector.redis.schema.support.RedisIntType;
import org.dbsyncer.connector.redis.schema.support.RedisLongType;
import org.dbsyncer.connector.redis.schema.support.RedisShortType;
import org.dbsyncer.connector.redis.schema.support.RedisStringType;
import org.dbsyncer.connector.redis.schema.support.RedisTimeType;
import org.dbsyncer.connector.redis.schema.support.RedisTimestampType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:18
 */
public final class RedisSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(new RedisStringType(), new RedisIntType(), new RedisShortType(), new RedisLongType(), new RedisDecimalType(), new RedisFloatType(), new RedisDoubleType(), new RedisDateType(), new RedisTimestampType(), new RedisTimeType(), new RedisBooleanType(), new RedisBytesType(), new RedisByteType())
                .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
                    if (mapping.containsKey(typeName)) {
                        throw new RedisException("Duplicate type name: " + typeName);
                    }
                    mapping.put(typeName, t);
                }));
    }
}