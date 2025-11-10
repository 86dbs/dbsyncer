package org.dbsyncer.connector.postgresql.schema;

import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBooleanType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBytesType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDateType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDecimalType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDoubleType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLEnumType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLFloatType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLIntType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLJsonType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLLongType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLStringType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLTextType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLTimestampType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLXmlType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * PostgreSQL标准数据类型解析器
 */
public final class PostgreSQLSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initStandardToTargetTypeMapping(Map<String, String> mapping) {
        // 文本
        mapping.put("STRING", "VARCHAR");
        mapping.put("UNICODE_STRING", "VARCHAR"); // PostgreSQL的VARCHAR默认支持UTF-8
        // 整型
        mapping.put("BYTE", "SMALLINT");
        mapping.put("UNSIGNED_BYTE", "INTEGER"); // PostgreSQL不支持unsigned，映射到更大的类型以避免溢出
        mapping.put("SHORT", "SMALLINT");
        mapping.put("UNSIGNED_SHORT", "INTEGER"); // PostgreSQL不支持unsigned，映射到更大的类型以避免溢出
        mapping.put("INT", "INTEGER");
        mapping.put("UNSIGNED_INT", "BIGINT"); // PostgreSQL不支持unsigned，映射到更大的类型以避免溢出
        mapping.put("LONG", "BIGINT");
        mapping.put("UNSIGNED_LONG", "NUMERIC"); // PostgreSQL不支持unsigned，映射到NUMERIC以避免溢出
        // 浮点型
        mapping.put("DECIMAL", "NUMERIC");
        mapping.put("UNSIGNED_DECIMAL", "NUMERIC"); // PostgreSQL不支持unsigned，但NUMERIC可以存储所有值
        mapping.put("DOUBLE", "DOUBLE PRECISION");
        mapping.put("FLOAT", "REAL");
        // 布尔型
        mapping.put("BOOLEAN", "BOOLEAN");
        // 时间
        mapping.put("DATE", "DATE");
        mapping.put("TIME", "TIME");
        mapping.put("TIMESTAMP", "TIMESTAMP");
        // 二进制
        mapping.put("BYTES", "BYTEA");
        // 结构化文本
        mapping.put("JSON", "JSON");
        mapping.put("XML", "XML");
        // 大文本
        mapping.put("TEXT", "TEXT");
        mapping.put("UNICODE_TEXT", "TEXT"); // PostgreSQL的TEXT默认支持UTF-8
        // 枚举和集合
        mapping.put("ENUM", "USER-DEFINED");
        mapping.put("SET", "VARCHAR");
        // UUID/GUID
        mapping.put("UUID", "UUID"); // PostgreSQL原生支持UUID类型
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new PostgreSQLStringType(),
                new PostgreSQLIntType(),
                new PostgreSQLLongType(),
                new PostgreSQLDecimalType(),
                new PostgreSQLFloatType(),
                new PostgreSQLDoubleType(),
                new PostgreSQLDateType(),
                new PostgreSQLTimestampType(),
                new PostgreSQLBooleanType(),
                new PostgreSQLBytesType(),
                new PostgreSQLJsonType(),    // 新增JSON类型支持
                new PostgreSQLEnumType(),    // 新增ENUM类型支持
                new PostgreSQLTextType(),    // 新增TEXT类型支持
                new PostgreSQLXmlType()      // 新增XML类型支持
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new PostgreSQLException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

    @Override
    protected String getDatabaseName() {
        return "PostgreSQL";
    }
}