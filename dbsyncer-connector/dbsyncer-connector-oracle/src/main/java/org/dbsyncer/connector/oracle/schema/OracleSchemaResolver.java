/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema;

import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.schema.support.*;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Oracle标准数据类型解析器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-04-05
 */
public final class OracleSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initStandardToTargetTypeMapping(Map<String, String> mapping) {
        // 文本
        mapping.put("STRING", "VARCHAR2");
        mapping.put("UNICODE_STRING", "NVARCHAR2");
        // 整型
        mapping.put("BYTE", "NUMBER");
        mapping.put("UNSIGNED_BYTE", "NUMBER"); // Oracle使用NUMBER表示所有数值类型
        mapping.put("SHORT", "NUMBER");
        mapping.put("UNSIGNED_SHORT", "NUMBER"); // Oracle使用NUMBER表示所有数值类型
        mapping.put("INT", "NUMBER");
        mapping.put("UNSIGNED_INT", "NUMBER"); // Oracle使用NUMBER表示所有数值类型
        mapping.put("LONG", "NUMBER");
        mapping.put("UNSIGNED_LONG", "NUMBER"); // Oracle使用NUMBER表示所有数值类型
        // 浮点型
        mapping.put("DECIMAL", "NUMBER");
        mapping.put("UNSIGNED_DECIMAL", "NUMBER"); // Oracle使用NUMBER表示所有数值类型
        mapping.put("DOUBLE", "BINARY_DOUBLE");
        mapping.put("FLOAT", "BINARY_FLOAT");
        // 布尔型
        mapping.put("BOOLEAN", "NUMBER");
        // 时间
        mapping.put("DATE", "DATE");
        mapping.put("TIME", "DATE");
        mapping.put("TIMESTAMP", "TIMESTAMP");
        // 二进制
        mapping.put("BYTES", "BLOB");
        // 结构化文本
        mapping.put("JSON", "CLOB"); // Oracle 12c+ 支持JSON，但存储为CLOB或VARCHAR2
        mapping.put("XML", "XMLTYPE");
        // 大文本
        mapping.put("TEXT", "CLOB");
        mapping.put("UNICODE_TEXT", "NCLOB");
        // 枚举和集合
        mapping.put("ENUM", "VARCHAR2"); // Oracle不支持ENUM，使用VARCHAR2存储
        mapping.put("SET", "VARCHAR2"); // Oracle不支持SET，使用VARCHAR2存储
        // UUID/GUID
        mapping.put("UUID", "VARCHAR2(36)"); // Oracle不支持UUID类型，使用VARCHAR2(36)存储UUID字符串
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new OracleStringType(),          // VARCHAR2, CHAR
                new OracleUnicodeStringType(),   // NVARCHAR2, NCHAR
                new OracleIntType(),
                new OracleLongType(),
                new OracleDecimalType(),
                new OracleFloatType(),
                new OracleDoubleType(),
                new OracleDateType(),
                new OracleTimestampType(),
                new OracleBytesType(),
                new OracleTextType(),            // CLOB
                new OracleUnicodeTextType(),     // NCLOB
                new OracleXmlType()              // XML类型支持
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new OracleException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

    @Override
    protected String getDatabaseName() {
        return "Oracle";
    }

}