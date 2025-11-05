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
        mapping.put("INT", "NUMBER");
        mapping.put("STRING", "VARCHAR2");
        mapping.put("TEXT", "CLOB");
        mapping.put("XML", "XMLTYPE");
        mapping.put("DECIMAL", "NUMBER");
        mapping.put("UNSIGNED_DECIMAL", "NUMBER"); // DECIMAL UNSIGNED → NUMBER（Oracle使用NUMBER表示所有数值类型）
        mapping.put("DATE", "DATE");
        mapping.put("TIME", "DATE");
        mapping.put("TIMESTAMP", "TIMESTAMP");
        mapping.put("BOOLEAN", "NUMBER");
        mapping.put("BYTE", "NUMBER");
        mapping.put("SHORT", "NUMBER");
        mapping.put("LONG", "NUMBER");
        // 无符号类型映射到NUMBER类型（Oracle使用NUMBER表示所有数值类型）
        mapping.put("UNSIGNED_BYTE", "NUMBER");   // TINYINT UNSIGNED (0-255) → NUMBER
        mapping.put("UNSIGNED_SHORT", "NUMBER"); // SMALLINT UNSIGNED (0-65535) → NUMBER
        mapping.put("UNSIGNED_INT", "NUMBER");    // INT UNSIGNED (0-4294967295) → NUMBER
        mapping.put("UNSIGNED_LONG", "NUMBER");   // BIGINT UNSIGNED (0-18446744073709551615) → NUMBER
        mapping.put("FLOAT", "BINARY_FLOAT");
        mapping.put("DOUBLE", "BINARY_DOUBLE");
        mapping.put("BYTES", "BLOB");
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new OracleStringType(),
                new OracleIntType(),
                new OracleLongType(),
                new OracleDecimalType(),
                new OracleFloatType(),
                new OracleDoubleType(),
                new OracleDateType(),
                new OracleTimestampType(),
                new OracleBytesType(),
                new OracleTextType(),    // 新增TEXT类型支持
                new OracleXmlType()      // 新增XML类型支持
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