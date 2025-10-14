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

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class OracleSchemaResolver extends AbstractSchemaResolver {
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

    /**
     * 获取标准类型编码
     */
    private int getStandardTypeCode(DataTypeEnum dataTypeEnum) {
        // 使用枚举的ordinal值作为类型编码
        return dataTypeEnum.ordinal();
    }
}
