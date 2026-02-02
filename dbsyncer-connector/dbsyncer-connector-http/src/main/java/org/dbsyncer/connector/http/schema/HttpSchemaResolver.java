/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.schema;

import org.dbsyncer.connector.http.HttpException;
import org.dbsyncer.connector.http.schema.support.HttpBooleanType;
import org.dbsyncer.connector.http.schema.support.HttpByteType;
import org.dbsyncer.connector.http.schema.support.HttpBytesType;
import org.dbsyncer.connector.http.schema.support.HttpDateType;
import org.dbsyncer.connector.http.schema.support.HttpDecimalType;
import org.dbsyncer.connector.http.schema.support.HttpDoubleType;
import org.dbsyncer.connector.http.schema.support.HttpFloatType;
import org.dbsyncer.connector.http.schema.support.HttpIntType;
import org.dbsyncer.connector.http.schema.support.HttpLongType;
import org.dbsyncer.connector.http.schema.support.HttpShortType;
import org.dbsyncer.connector.http.schema.support.HttpStringType;
import org.dbsyncer.connector.http.schema.support.HttpTimestampType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:18
 */
public final class HttpSchemaResolver extends AbstractSchemaResolver {
    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new HttpStringType(),
                new HttpIntType(),
                new HttpShortType(),
                new HttpLongType(),
                new HttpDecimalType(),
                new HttpFloatType(),
                new HttpDoubleType(),
                new HttpDateType(),
                new HttpTimestampType(),
                new HttpBooleanType(),
                new HttpBytesType(),
                new HttpByteType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new HttpException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}