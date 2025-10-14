/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema;

import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.schema.support.*;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class OracleSchemaResolver extends AbstractSchemaResolver {

    // Java标准类型到Oracle特定类型的映射
    private static final Map<String, String> STANDARD_TO_TARGET_TYPE_MAP = new HashMap<>();
    
    static {
        STANDARD_TO_TARGET_TYPE_MAP.put("INT", "NUMBER");
        STANDARD_TO_TARGET_TYPE_MAP.put("STRING", "VARCHAR2");
        STANDARD_TO_TARGET_TYPE_MAP.put("DECIMAL", "NUMBER");
        STANDARD_TO_TARGET_TYPE_MAP.put("DATE", "DATE");
        STANDARD_TO_TARGET_TYPE_MAP.put("TIME", "DATE");
        STANDARD_TO_TARGET_TYPE_MAP.put("TIMESTAMP", "TIMESTAMP");
        STANDARD_TO_TARGET_TYPE_MAP.put("BOOLEAN", "NUMBER");
        STANDARD_TO_TARGET_TYPE_MAP.put("BYTE", "NUMBER");
        STANDARD_TO_TARGET_TYPE_MAP.put("SHORT", "NUMBER");
        STANDARD_TO_TARGET_TYPE_MAP.put("LONG", "NUMBER");
        STANDARD_TO_TARGET_TYPE_MAP.put("FLOAT", "BINARY_FLOAT");
        STANDARD_TO_TARGET_TYPE_MAP.put("DOUBLE", "BINARY_DOUBLE");
        STANDARD_TO_TARGET_TYPE_MAP.put("BYTES", "RAW");
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new OracleBytesType(),
                new OracleDateType(),
                new OracleDecimalType(),
                new OracleDoubleType(),
                new OracleFloatType(),
                new OracleIntType(),
                new OracleLongType(),
                new OracleStringType(),
                new OracleTimestampType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new OracleException("Duplicate type name: " + typeName);
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
            String.format("Unsupported Oracle type: %s. Please add mapping for this type in DataType configuration.",
                         field.getTypeName()));
    }

    @Override
    public Field fromStandardType(Field standardField) {
        // 将Java标准类型转换为Oracle特定类型
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
     * 获取目标类型名称（将Java标准类型转换为Oracle特定类型）
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
