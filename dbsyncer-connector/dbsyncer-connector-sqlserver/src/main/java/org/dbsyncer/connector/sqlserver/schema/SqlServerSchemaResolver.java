/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema;

import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.connector.sqlserver.schema.support.*;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * SqlServer标准数据类型解析器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-04-05
 */
public final class SqlServerSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new SqlServerBooleanType(),
                new SqlServerBytesType(),
                new SqlServerByteType(),
                new SqlServerDateType(),
                new SqlServerDecimalType(),
                new SqlServerDoubleType(),
                new SqlServerFloatType(),
                new SqlServerIntType(),
                new SqlServerLongType(),
                new SqlServerShortType(),
                new SqlServerStringType(),
                new SqlServerTimestampType(),
                new SqlServerTimeType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new SqlServerException("Duplicate type name: " + typeName);
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
            String.format("Unsupported SQL Server type: %s. Please add mapping for this type in DataType configuration.",
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