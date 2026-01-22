/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema;

import org.dbsyncer.connector.sqlite.SQLiteException;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteBytesType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteDoubleType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteIntType;
import org.dbsyncer.connector.sqlite.schema.support.SQLiteStringType;
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
                new SQLiteDoubleType(),
                new SQLiteBytesType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new SQLiteException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}