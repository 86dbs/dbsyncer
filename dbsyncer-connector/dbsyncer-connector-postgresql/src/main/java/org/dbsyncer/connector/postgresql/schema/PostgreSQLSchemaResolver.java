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
        mapping.put("INT", "INTEGER");
        mapping.put("STRING", "VARCHAR");
        mapping.put("UNICODE_STRING", "VARCHAR"); // PostgreSQL的VARCHAR默认支持UTF-8
        mapping.put("TEXT", "TEXT");
        mapping.put("UNICODE_TEXT", "TEXT"); // PostgreSQL的TEXT默认支持UTF-8
        mapping.put("JSON", "JSON");
        mapping.put("XML", "XML");
        mapping.put("ENUM", "USER-DEFINED");
        mapping.put("SET", "VARCHAR");
        mapping.put("DECIMAL", "NUMERIC");
        mapping.put("UNSIGNED_DECIMAL", "NUMERIC"); // DECIMAL UNSIGNED → NUMERIC（PostgreSQL不支持unsigned，但NUMERIC可以存储所有值）
        mapping.put("DATE", "DATE");
        mapping.put("TIME", "TIME");
        mapping.put("TIMESTAMP", "TIMESTAMP");
        mapping.put("BOOLEAN", "BOOLEAN");
        mapping.put("BYTE", "SMALLINT");
        mapping.put("SHORT", "SMALLINT");
        mapping.put("LONG", "BIGINT");
        // 无符号类型映射到更大的类型以避免溢出
        mapping.put("UNSIGNED_BYTE", "INTEGER");   // TINYINT UNSIGNED (0-255) → INTEGER
        mapping.put("UNSIGNED_SHORT", "INTEGER"); // SMALLINT UNSIGNED (0-65535) → INTEGER
        mapping.put("UNSIGNED_INT", "BIGINT");    // INT UNSIGNED (0-4294967295) → BIGINT
        mapping.put("UNSIGNED_LONG", "NUMERIC");  // BIGINT UNSIGNED (0-18446744073709551615) → NUMERIC
        mapping.put("FLOAT", "REAL");
        mapping.put("DOUBLE", "DOUBLE PRECISION");
        mapping.put("BYTES", "BYTEA");
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