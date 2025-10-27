/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema;

import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBooleanType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBytesType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDateType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDecimalType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDoubleType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLFloatType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLIntType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLLongType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLStringType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLTimestampType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * PostgreSQL标准数据类型解析器
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:01
 */
public final class PostgreSQLSchemaResolver extends AbstractSchemaResolver {

    // Java标准类型到PostgreSQL特定类型的映射
    private static final Map<String, String> STANDARD_TO_TARGET_TYPE_MAP = new HashMap<>();
    
    static {
        STANDARD_TO_TARGET_TYPE_MAP.put("INT", "integer");
        STANDARD_TO_TARGET_TYPE_MAP.put("STRING", "varchar");
        STANDARD_TO_TARGET_TYPE_MAP.put("DECIMAL", "numeric");
        STANDARD_TO_TARGET_TYPE_MAP.put("DATE", "date");
        STANDARD_TO_TARGET_TYPE_MAP.put("TIME", "time");
        STANDARD_TO_TARGET_TYPE_MAP.put("TIMESTAMP", "timestamp");
        STANDARD_TO_TARGET_TYPE_MAP.put("BOOLEAN", "boolean");
        STANDARD_TO_TARGET_TYPE_MAP.put("BYTE", "smallint");
        STANDARD_TO_TARGET_TYPE_MAP.put("SHORT", "smallint");
        STANDARD_TO_TARGET_TYPE_MAP.put("LONG", "bigint");
        STANDARD_TO_TARGET_TYPE_MAP.put("FLOAT", "real");
        STANDARD_TO_TARGET_TYPE_MAP.put("DOUBLE", "double precision");
        STANDARD_TO_TARGET_TYPE_MAP.put("BYTES", "bytea");
        STANDARD_TO_TARGET_TYPE_MAP.put("ARRAY", "text[]");
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
                new PostgreSQLBytesType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new PostgreSQLException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

    @Override
    public Field toStandardType(Field field) {
        // 利用现有的DataType映射机制进行类型转换
        DataType dataType = getDataType(field);
        if (dataType != null) {
            // 使用DataType的getType()方法获取标准类型
            return new Field(field.getName(),
                           dataType.getType().name(),
                           getStandardTypeCode(dataType.getType()),
                           field.isPk(),
                           field.getColumnSize(),
                           field.getRatio());
        }

        // 如果没有找到对应的DataType，抛出异常以发现系统不足
        throw new UnsupportedOperationException(
            String.format("Unsupported PostgreSQL type: %s. Please add mapping for this type in DataType configuration.",
                         field.getTypeName()));
    }

    @Override
    public Field fromStandardType(Field standardField) {
        // 将Java标准类型转换为PostgreSQL特定类型
        String targetTypeName = getTargetTypeName(standardField.getTypeName());
        return new Field(standardField.getName(),
                       targetTypeName,
                       getTargetTypeCode(targetTypeName),
                       standardField.isPk(),
                       standardField.getColumnSize(),
                       standardField.getRatio());
    }

    /**
     * 获取标准类型编码
     */
    private int getStandardTypeCode(DataTypeEnum dataTypeEnum) {
        // 使用枚举的ordinal值作为类型编码
        return dataTypeEnum.ordinal();
    }

    /**
     * 获取目标类型名称（将Java标准类型转换为PostgreSQL特定类型）
     */
    private String getTargetTypeName(String standardTypeName) {
        return STANDARD_TO_TARGET_TYPE_MAP.getOrDefault(standardTypeName, standardTypeName);
    }

    /**
     * 获取目标类型编码
     */
    private int getTargetTypeCode(String targetTypeName) {
        // 这里可以根据需要实现类型编码映射
        // 暂时返回0，实际使用时可能需要更精确的映射
        return 0;
    }
}