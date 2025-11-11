package org.dbsyncer.connector.mysql.schema;

import org.dbsyncer.connector.mysql.MySQLException;
import org.dbsyncer.connector.mysql.schema.support.*;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * MySQL标准数据类型解析器
 * <p>https://gitee.com/ghi/dbsyncer/wikis/%E9%A1%B9%E7%9B%AE%E8%AE%BE%E8%AE%A1/%E6%A0%87%E5%87%86%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B/MySQL</p>
 */
public final class MySQLSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initStandardToTargetTypeMapping(Map<String, String> mapping) {
        // 文本
        mapping.put("STRING", "VARCHAR");
        mapping.put("UNICODE_STRING", "VARCHAR"); // MySQL的VARCHAR默认支持UTF-8
        // 整型
        mapping.put("BYTE", "TINYINT");
        mapping.put("UNSIGNED_BYTE", "TINYINT UNSIGNED");
        mapping.put("SHORT", "SMALLINT");
        mapping.put("UNSIGNED_SHORT", "SMALLINT UNSIGNED");
        mapping.put("INT", "INT");
        mapping.put("UNSIGNED_INT", "INT UNSIGNED");
        mapping.put("LONG", "BIGINT");
        mapping.put("UNSIGNED_LONG", "BIGINT UNSIGNED");
        // 浮点型
        mapping.put("DECIMAL", "DECIMAL");
        mapping.put("UNSIGNED_DECIMAL", "DECIMAL UNSIGNED");
        mapping.put("DOUBLE", "DOUBLE");
        mapping.put("FLOAT", "FLOAT");
        // 布尔型
        mapping.put("BOOLEAN", "TINYINT");
        // 时间
        mapping.put("DATE", "DATE");
        mapping.put("TIME", "TIME");
        mapping.put("TIMESTAMP", "DATETIME");
        // 二进制
        mapping.put("BYTES", "VARBINARY");
        // 大容量二进制
        mapping.put("BLOB", "BLOB");
        // 结构化文本
        mapping.put("JSON", "JSON");
        mapping.put("XML", "LONGTEXT");
        // 大文本
        mapping.put("TEXT", "TEXT");
        mapping.put("UNICODE_TEXT", "TEXT"); // MySQL的TEXT默认支持UTF-8
        // 枚举和集合
        mapping.put("ENUM", "ENUM");
        mapping.put("SET", "SET");
        // UUID/GUID
        mapping.put("UUID", "CHAR(36)"); // MySQL使用CHAR(36)存储UUID字符串
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new MySQLBytesType(),
                new MySQLBlobType(),  // 新增BLOB类型支持
                new MySQLByteType(),
                new MySQLDateType(),
                new MySQLDecimalType(),
                new MySQLDoubleType(),
                new MySQLEnumType(),   // 新增ENUM类型支持
                new MySQLFloatType(),
                new MySQLGeometryType(),  // 新增GEOMETRY类型支持
                new MySQLIntType(),
                new MySQLJsonType(),  // 新增JSON类型支持
                new MySQLLongType(),
                new MySQLSetType(),   // 新增SET类型支持
                new MySQLShortType(),
                new MySQLStringType(),
                new MySQLTextType(),  // 新增TEXT类型支持
                new MySQLTimestampType(),
                new MySQLTimeType(),
                new MySQLUnsignedByteType(),    // 新增无符号字节类型支持
                new MySQLUnsignedShortType(),   // 新增无符号短整型支持
                new MySQLUnsignedIntType(),     // 新增无符号整型支持
                new MySQLUnsignedLongType(),    // 新增无符号长整型支持
                new MySQLUnsignedDecimalType()  // 新增无符号精确小数类型支持
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new MySQLException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

    @Override
    protected String getDatabaseName() {
        return "MySQL";
    }

}