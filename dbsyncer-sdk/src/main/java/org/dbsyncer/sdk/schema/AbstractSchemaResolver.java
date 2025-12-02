/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 20:50
 */
public abstract class AbstractSchemaResolver implements SchemaResolver {

    private final Map<String, DataType> mapping = new ConcurrentHashMap<>();
    protected final Map<String, String> standardToTargetTypeMap = new ConcurrentHashMap<>();

    public AbstractSchemaResolver() {
        initStandardToTargetTypeMapping(standardToTargetTypeMap);
        initDataTypeMapping(mapping);
        Assert.notEmpty(mapping, "At least one data type is required.");
    }

    /**
     * 初始化标准类型到目标数据库类型的映射表
     *
     * @param mapping 映射表，由子类填充
     */
    protected abstract void initStandardToTargetTypeMapping(Map<String, String> mapping);

    /**
     * 初始化数据类型映射
     *
     * @param mapping 映射表，由子类填充
     */
    protected abstract void initDataTypeMapping(Map<String, DataType> mapping);

    /**
     * 获取数据库名称，用于异常消息
     *
     * @return 数据库名称
     */
    protected abstract String getDatabaseName();

    protected DataType getDataType(Field field) {
        return mapping.get(field.getTypeName());
    }

    /**
     * 获取标准类型编码
     *
     * @param dataTypeEnum 数据类型枚举
     * @return 类型编码
     */
    protected int getStandardTypeCode(DataTypeEnum dataTypeEnum) {
        return dataTypeEnum.ordinal();
    }

    /**
     * 获取目标类型编码
     *
     * @param targetTypeName 目标类型名称
     * @return 类型编码（默认返回0）
     */
    protected int getTargetTypeCode(String targetTypeName) {
        // 这里可以根据需要实现类型编码映射
        // 暂时返回0，实际使用时可能需要更精确的映射
        return 0;
    }

    /**
     * 获取目标类型名称（将标准类型转换为目标数据库类型）
     *
     * @param standardTypeName 标准类型名称
     * @return 目标数据库类型名称
     */
    protected String getTargetTypeName(String standardTypeName) {
        return standardToTargetTypeMap.getOrDefault(standardTypeName, getDefaultTargetTypeName(standardTypeName));
    }

    /**
     * 获取默认目标类型名称（当映射表中不存在时）
     * 子类可以重写此方法以提供特殊处理（如 SQLite 的 toUpperCase()）
     *
     * @param standardTypeName 标准类型名称
     * @return 默认目标类型名称
     */
    protected String getDefaultTargetTypeName(String standardTypeName) {
        return standardTypeName;
    }

    @Override
    public Object merge(Object val, Field field) {
        DataType dataType = getDataType(field);
        if (dataType != null) {
            return dataType.mergeValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] merge into [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Object convert(Object val, Field field) {
        DataType dataType = getDataType(field);
        if (dataType != null) {
            return dataType.convertValue(val, field);
        }
        throw new SdkException(String.format("%s does not support type [%s] convert to [%s], val [%s]", getClass().getSimpleName(), val.getClass(), field.getTypeName(), val));
    }

    @Override
    public Field toStandardType(Field field) {
        // 利用现有的DataType映射机制进行类型转换
        DataType dataType = getDataType(field);
        if (dataType != null) {
            // 使用DataType的getType()方法获取标准类型
            Field result = new Field(field.getName(),
                    dataType.getType().name(),
                    getStandardTypeCode(dataType.getType()),
                    field.isPk(),
                    field.getColumnSize(),
                    field.getRatio());
            // 保留SRID信息（用于Geometry类型）
            if (field.getSrid() != null) {
                result.setSrid(field.getSrid());
            }
            // 保留字段的其他元数据属性（这些属性在类型转换时应该保持不变）
            result.setNullable(field.getNullable());
            result.setDefaultValue(field.getDefaultValue());
            result.setComment(field.getComment());
            result.setAutoincrement(field.isAutoincrement());
            result.setIsSizeFixed(field.getIsSizeFixed());
            return result;
        }

        // 如果没有找到对应的DataType，抛出异常以发现系统不足
        throw new UnsupportedOperationException(
                String.format("Unsupported %s type: %s. Please add mapping for this type in DataType configuration.",
                        getDatabaseName(), field.getTypeName()));
    }

    @Override
    public Field fromStandardType(Field standardField) {
        // 将Java标准类型转换为目标数据库特定类型
        String targetTypeName = getTargetTypeName(standardField.getTypeName());
        Field result = new Field(standardField.getName(),
                targetTypeName,
                getTargetTypeCode(targetTypeName),
                standardField.isPk(),
                standardField.getColumnSize(),
                standardField.getRatio());
        // 保留SRID信息（用于Geometry类型）
        if (standardField.getSrid() != null) {
            result.setSrid(standardField.getSrid());
        }
        return result;
    }

    @Override
    public Field toStandardTypeFromDDL(Field field, ColDataType colDataType) {
        // 利用现有的DataType映射机制进行类型转换
        DataType dataType = getDataType(field);
        if (dataType != null) {
            // 使用DataType的getType()方法获取标准类型
            Field result = new Field(field.getName(),
                    dataType.getType().name(),
                    getStandardTypeCode(dataType.getType()),
                    field.isPk(),
                    field.getColumnSize(),
                    field.getRatio());

            // 让具体的DataType处理DDL参数，获取尺寸和精度信息
            Field ddlField = dataType.handleDDLParameters(colDataType);

            // 将DDL参数处理结果合并到最终结果中
            result.setColumnSize(ddlField.getColumnSize());
            result.setRatio(ddlField.getRatio());
            // 合并SRID信息（用于Geometry类型）
            if (ddlField.getSrid() != null) {
                result.setSrid(ddlField.getSrid());
            }
            // 合并isSizeFixed信息（用于区分固定长度和可变长度字符串/二进制）
            if (ddlField.getIsSizeFixed() != null) {
                result.setIsSizeFixed(ddlField.getIsSizeFixed());
            }

            return result;
        }

        // 如果没有找到对应的DataType，抛出异常以发现系统不足
        throw new UnsupportedOperationException(
                String.format("Unsupported %s type: %s. Please add mapping for this type in DataType configuration.",
                        getDatabaseName(), field.getTypeName()));
    }

}