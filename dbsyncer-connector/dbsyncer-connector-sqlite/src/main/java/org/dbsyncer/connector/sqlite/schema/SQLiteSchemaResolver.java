/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema;

import org.dbsyncer.connector.sqlite.SQLiteException;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteBooleanType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteByteType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteBytesType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteDateType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteDecimalType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteDoubleType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteFloatType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteIntType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteLongType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteShortType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteStringType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteTimeType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteTimestampType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-13 00:08
 */
public final class SQLiteSchemaResolver extends AbstractSchemaResolver {
    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new SQLiteStringType(),
                new SQLiteIntType(),
                new SQLiteShortType(),
                new SQLiteLongType(),
                new SQLiteDecimalType(),
                new SQLiteFloatType(),
                new SQLiteDoubleType(),
                new SQLiteDateType(),
                new SQLiteTimestampType(),
                new SQLiteTimeType(),
                new SQLiteBooleanType(),
                new SQLiteBytesType(),
                new SQLiteByteType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new SQLiteException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}