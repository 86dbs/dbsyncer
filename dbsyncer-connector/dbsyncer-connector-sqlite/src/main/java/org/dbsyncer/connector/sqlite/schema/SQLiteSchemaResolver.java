/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema;

import org.dbsyncer.connector.sqlite.SQLiteException;
import org.dbsyncer.connector.sqlite.schema.support.*;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * SQLite标准数据类型解析器
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteSchemaResolver extends AbstractSchemaResolver {

    // Java标准类型到SQLite特定类型的映射
    private static final Map<String, String> STANDARD_TO_TARGET_TYPE_MAP = new HashMap<>();
    
    static {
        STANDARD_TO_TARGET_TYPE_MAP.put("INT", "INTEGER");
        STANDARD_TO_TARGET_TYPE_MAP.put("STRING", "TEXT");
        STANDARD_TO_TARGET_TYPE_MAP.put("DECIMAL", "REAL");
        STANDARD_TO_TARGET_TYPE_MAP.put("DATE", "TEXT");
        STANDARD_TO_TARGET_TYPE_MAP.put("TIME", "TEXT");
        STANDARD_TO_TARGET_TYPE_MAP.put("TIMESTAMP", "TEXT");
        STANDARD_TO_TARGET_TYPE_MAP.put("BOOLEAN", "INTEGER");
        STANDARD_TO_TARGET_TYPE_MAP.put("BYTE", "INTEGER");
        STANDARD_TO_TARGET_TYPE_MAP.put("SHORT", "INTEGER");
        STANDARD_TO_TARGET_TYPE_MAP.put("LONG", "INTEGER");
        STANDARD_TO_TARGET_TYPE_MAP.put("FLOAT", "REAL");
        STANDARD_TO_TARGET_TYPE_MAP.put("DOUBLE", "REAL");
        STANDARD_TO_TARGET_TYPE_MAP.put("BYTES", "BLOB");
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new SQLiteTextType(),      // TEXT 存储类 - 文本亲和性
                new SQLiteIntegerType(),   // INTEGER 存储类 - 整数亲和性
                new SQLiteRealType(),      // REAL 存储类 - 实数亲和性
                new SQLiteBlobType()       // BLOB 存储类 - 二进制亲和性
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new SQLiteException("Duplicate type name: " + typeName);
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
            String.format("Unsupported SQLite type: %s. Please add mapping for this type in DataType configuration.",
                         field.getTypeName()));
    }

    @Override
    public Field fromStandardType(Field standardField) {
        // 将Java标准类型转换为SQLite特定类型
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
     * 获取目标类型名称（将Java标准类型转换为SQLite特定类型）
     */
    private String getTargetTypeName(String standardTypeName) {
        return STANDARD_TO_TARGET_TYPE_MAP.getOrDefault(standardTypeName, standardTypeName.toUpperCase());
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
