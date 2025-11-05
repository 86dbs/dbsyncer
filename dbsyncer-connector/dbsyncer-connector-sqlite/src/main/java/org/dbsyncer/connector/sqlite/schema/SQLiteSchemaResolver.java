/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema;

import org.dbsyncer.connector.sqlite.SQLiteException;
import org.dbsyncer.connector.sqlite.schema.support.*;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * SQLite标准数据类型解析器
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initStandardToTargetTypeMapping(Map<String, String> mapping) {
        mapping.put("INT", "INTEGER");
        mapping.put("STRING", "TEXT");
        mapping.put("DECIMAL", "REAL");
        mapping.put("DATE", "TEXT");
        mapping.put("TIME", "TEXT");
        mapping.put("TIMESTAMP", "TEXT");
        mapping.put("BOOLEAN", "INTEGER");
        mapping.put("BYTE", "INTEGER");
        mapping.put("SHORT", "INTEGER");
        mapping.put("LONG", "INTEGER");
        mapping.put("FLOAT", "REAL");
        mapping.put("DOUBLE", "REAL");
        mapping.put("BYTES", "BLOB");
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new SQLiteTextType(),      // TEXT 存储类 - 文本亲和性
                new SQLiteIntegerType(),   // INTEGER 存储类 - 整数亲和性
                new SQLiteRealType(),      // REAL 存储类 - 实数亲和性
                new SQLiteBlobType()       // BLOB 存储类 - 二进制亲和性
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new SQLiteException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

    @Override
    protected String getDatabaseName() {
        return "SQLite";
    }

    @Override
    protected String getDefaultTargetTypeName(String standardTypeName) {
        // SQLite 特殊处理：将未映射的类型转换为大写
        return standardTypeName.toUpperCase();
    }
}
