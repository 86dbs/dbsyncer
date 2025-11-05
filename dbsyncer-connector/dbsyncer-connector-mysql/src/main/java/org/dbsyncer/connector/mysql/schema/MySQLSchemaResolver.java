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
        mapping.put("INT", "INT");
        mapping.put("STRING", "VARCHAR");
        mapping.put("TEXT", "TEXT");
        mapping.put("JSON", "JSON");
        mapping.put("XML", "LONGTEXT");
        mapping.put("ENUM", "ENUM");
        mapping.put("SET", "SET");
        mapping.put("DECIMAL", "DECIMAL");
        mapping.put("DATE", "DATE");
        mapping.put("TIME", "TIME");
        mapping.put("TIMESTAMP", "DATETIME");
        mapping.put("BOOLEAN", "TINYINT");
        mapping.put("BYTE", "TINYINT");
        mapping.put("SHORT", "SMALLINT");
        mapping.put("LONG", "BIGINT");
        mapping.put("FLOAT", "FLOAT");
        mapping.put("DOUBLE", "DOUBLE");
        mapping.put("BYTES", "VARBINARY");
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new MySQLBytesType(),
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
                new MySQLTimeType()
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