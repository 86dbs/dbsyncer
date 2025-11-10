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
        // 文本
        mapping.put("STRING", "varchar");
        mapping.put("UNICODE_STRING", "nvarchar");
        // 整型
        mapping.put("BYTE", "tinyint");
        mapping.put("UNSIGNED_BYTE", "int"); // SQL Server不支持unsigned，映射到更大的类型以避免溢出
        mapping.put("SHORT", "smallint");
        mapping.put("UNSIGNED_SHORT", "int"); // SQL Server不支持unsigned，映射到更大的类型以避免溢出
        mapping.put("INT", "int");
        mapping.put("UNSIGNED_INT", "bigint"); // SQL Server不支持unsigned，映射到更大的类型以避免溢出
        mapping.put("LONG", "bigint");
        mapping.put("UNSIGNED_LONG", "decimal"); // SQL Server不支持unsigned，映射到decimal以避免溢出
        // 浮点型
        mapping.put("DECIMAL", "decimal");
        mapping.put("UNSIGNED_DECIMAL", "decimal"); // SQL Server不支持unsigned，但decimal可以存储所有值
        mapping.put("DOUBLE", "float");
        mapping.put("FLOAT", "real");
        // 布尔型
        mapping.put("BOOLEAN", "bit");
        // 时间
        mapping.put("DATE", "date");
        mapping.put("TIME", "time");
        mapping.put("TIMESTAMP", "datetime2"); // 注意：这是标准TIMESTAMP（日期时间类型），SQL Server的TIMESTAMP类型由SqlServerExactNumericType处理，映射为LONG
        // 二进制
        mapping.put("BYTES", "varbinary");
        // 结构化文本
        mapping.put("JSON", "nvarchar"); // SQL Server 2016+ 支持JSON，但存储为NVARCHAR(MAX)
        mapping.put("XML", "xml");
        // 大文本
        mapping.put("TEXT", "varchar");
        mapping.put("UNICODE_TEXT", "nvarchar");
        // 枚举和集合
        mapping.put("ENUM", "nvarchar"); // SQL Server不支持ENUM，使用nvarchar存储
        mapping.put("SET", "nvarchar"); // SQL Server不支持SET，使用nvarchar存储
        // UUID/GUID
        mapping.put("UUID", "uniqueidentifier"); // SQL Server原生支持UNIQUEIDENTIFIER类型
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new SqlServerExactNumericType(),        // 精确数值类型（整数类型，包括TIMESTAMP）
                new SqlServerDecimalType(),             // Decimal类型（精确小数类型）
                new SqlServerApproximateNumericType(),  // 近似数值类型
                new SqlServerDateTimeType(),            // 日期时间类型
                new SqlServerStringType(),              // 字符字符串类型（CHAR, VARCHAR）
                new SqlServerUnicodeStringType(),       // Unicode字符字符串类型（NCHAR, NVARCHAR）
                new SqlServerBinaryStringType(),        // 二进制字符串类型
                new SqlServerTextType(),                // TEXT类型支持
                new SqlServerUnicodeTextType(),         // Unicode TEXT类型支持（NTEXT）
                new SqlServerXmlType(),                 // XML类型支持
                new SqlServerUniqueIdentifierType()     // UNIQUEIDENTIFIER类型支持
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