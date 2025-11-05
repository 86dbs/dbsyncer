package org.dbsyncer.connector.sqlserver.schema;

import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.connector.sqlserver.schema.support.*;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * SqlServer标准数据类型解析器
 */
public final class SqlServerSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initStandardToTargetTypeMapping(Map<String, String> mapping) {
        mapping.put("INT", "int");
        mapping.put("STRING", "nvarchar");
        mapping.put("TEXT", "nvarchar");
        mapping.put("XML", "xml");
        mapping.put("DECIMAL", "decimal");
        mapping.put("UNSIGNED_DECIMAL", "decimal"); // DECIMAL UNSIGNED → decimal（SQL Server不支持unsigned，但decimal可以存储所有值）
        mapping.put("DATE", "date");
        mapping.put("TIME", "time");
        mapping.put("TIMESTAMP", "datetime2");
        mapping.put("BOOLEAN", "bit");
        mapping.put("BYTE", "tinyint");
        mapping.put("SHORT", "smallint");
        mapping.put("LONG", "bigint");
        // 无符号类型映射到更大的类型以避免溢出
        mapping.put("UNSIGNED_BYTE", "int");      // TINYINT UNSIGNED (0-255) → INT
        mapping.put("UNSIGNED_SHORT", "int");    // SMALLINT UNSIGNED (0-65535) → INT
        mapping.put("UNSIGNED_INT", "bigint");   // INT UNSIGNED (0-4294967295) → BIGINT
        mapping.put("UNSIGNED_LONG", "decimal"); // BIGINT UNSIGNED (0-18446744073709551615) → DECIMAL
        mapping.put("FLOAT", "real");
        mapping.put("DOUBLE", "float");
        mapping.put("BYTES", "varbinary");
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new SqlServerExactNumericType(),        // 精确数值类型（整数类型）
                new SqlServerDecimalType(),             // Decimal类型（精确小数类型）
                new SqlServerApproximateNumericType(),  // 近似数值类型
                new SqlServerDateTimeType(),            // 日期时间类型
                new SqlServerStringType(),              // 字符字符串类型
                new SqlServerBinaryStringType(),        // 二进制字符串类型
                new SqlServerTextType(),                // 新增TEXT类型支持
                new SqlServerXmlType()                  // 新增XML类型支持
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new SqlServerException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

    @Override
    protected String getDatabaseName() {
        return "SQL Server";
    }

}