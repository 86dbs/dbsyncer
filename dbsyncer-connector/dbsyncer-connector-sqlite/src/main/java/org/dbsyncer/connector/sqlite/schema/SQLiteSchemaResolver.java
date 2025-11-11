package org.dbsyncer.connector.sqlite.schema;

import org.dbsyncer.connector.sqlite.SQLiteException;
import org.dbsyncer.connector.sqlite.schema.support.*;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * SQLite标准数据类型解析器
 */
public final class SQLiteSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initStandardToTargetTypeMapping(Map<String, String> mapping) {
        // 文本
        mapping.put("STRING", "TEXT");
        mapping.put("UNICODE_STRING", "TEXT"); // SQLite的TEXT默认支持UTF-8
        // 整型
        mapping.put("BYTE", "INTEGER");
        mapping.put("UNSIGNED_BYTE", "INTEGER"); // SQLite不区分有符号/无符号，但INTEGER可以存储更大范围的值
        mapping.put("SHORT", "INTEGER");
        mapping.put("UNSIGNED_SHORT", "INTEGER"); // SQLite不区分有符号/无符号，但INTEGER可以存储更大范围的值
        mapping.put("INT", "INTEGER");
        mapping.put("UNSIGNED_INT", "INTEGER"); // SQLite不区分有符号/无符号，但INTEGER可以存储更大范围的值
        mapping.put("LONG", "INTEGER");
        mapping.put("UNSIGNED_LONG", "INTEGER"); // SQLite不区分有符号/无符号，但INTEGER可以存储更大范围的值
        // 浮点型
        mapping.put("DECIMAL", "REAL");
        mapping.put("UNSIGNED_DECIMAL", "REAL"); // SQLite不区分有符号/无符号，但REAL可以存储所有值
        mapping.put("DOUBLE", "REAL");
        mapping.put("FLOAT", "REAL");
        // 布尔型
        mapping.put("BOOLEAN", "INTEGER");
        // 时间
        mapping.put("DATE", "TEXT");
        mapping.put("TIME", "TEXT");
        mapping.put("TIMESTAMP", "TEXT");
        // 二进制
        mapping.put("BYTES", "BLOB"); // SQLite的BLOB类型用于二进制数据
        // 大容量二进制
        mapping.put("BLOB", "BLOB"); // SQLite使用BLOB存储大容量二进制数据
        // 结构化文本
        mapping.put("JSON", "TEXT"); // SQLite 3.38+ 支持JSON函数，但存储仍为TEXT
        mapping.put("XML", "TEXT");
        // 大文本
        mapping.put("TEXT", "TEXT");
        mapping.put("UNICODE_TEXT", "TEXT"); // SQLite的TEXT默认支持UTF-8
        // 枚举和集合
        mapping.put("ENUM", "TEXT");
        mapping.put("SET", "TEXT");
        // UUID/GUID
        mapping.put("UUID", "TEXT"); // SQLite不支持UUID类型，使用TEXT存储UUID字符串
        // 空间几何类型
        mapping.put("GEOMETRY", "BLOB"); // SQLite通过SpatiaLite扩展支持GEOMETRY类型，或使用BLOB存储WKB格式
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                // TEXT 存储类 - 文本亲和性
                new SQLiteUnicodeStringType(),  // VARCHAR, CHAR, NCHAR, NVARCHAR - Unicode文本类型（SQLite无长度限制，标准化为UNICODE_TEXT）
                new SQLiteTextType(),           // TEXT, CLOB - Unicode大文本类型
                // INTEGER 存储类 - 整数亲和性
                new SQLiteIntegerType(),        // INTEGER, INT, TINYINT, SMALLINT, MEDIUMINT, BIGINT, BOOLEAN
                // 无符号整数类型（SQLite不区分有符号/无符号，但支持标准类型转换）
                new SQLiteUnsignedByteType(),   // TINYINT UNSIGNED
                new SQLiteUnsignedShortType(),  // SMALLINT UNSIGNED
                new SQLiteUnsignedIntType(),    // INT UNSIGNED, INTEGER UNSIGNED, MEDIUMINT UNSIGNED
                new SQLiteUnsignedLongType(),   // BIGINT UNSIGNED
                // REAL 存储类 - 实数亲和性
                new SQLiteRealType(),           // REAL, DOUBLE, FLOAT, NUMERIC, DECIMAL
                new SQLiteUnsignedDecimalType(), // DECIMAL UNSIGNED
                // BLOB 存储类 - 二进制亲和性
                new SQLiteBlobType(),           // BLOB
                new SQLiteGeometryType(),       // GEOMETRY类型支持（通过SpatiaLite扩展或BLOB存储）
                // 日期时间类型（SQLite存储为TEXT）
                new SQLiteDateType(),           // DATE, DATETIME
                new SQLiteTimeType(),           // TIME
                new SQLiteTimestampType()       // TIMESTAMP
                // 注意：SQLite 不原生支持 ENUM、SET、JSON、XML 类型
                // 这些标准类型通过 initStandardToTargetTypeMapping() 映射到 TEXT 类型
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
