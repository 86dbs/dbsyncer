/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;

import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-23 23:00
 */
public interface DataType {

    /**
     * 获取支持的转换类型
     *
     * @return
     */
    Set<String> getSupportedTypeName();

    /**
     * 转换为标准数据类型
     *
     * @param val   转换值
     * @param field 数据类型
     * @return Object
     */
    Object mergeValue(Object val, Field field);

    /**
     * 转换为指定数据类型
     *
     * @param val   转换值
     * @param field 数据类型
     * @return Object
     */
    Object convertValue(Object val, Field field);

    /**
     * 获取数据类型
     *
     * @return
     */
    DataTypeEnum getType();
    
    /**
     * 处理DDL解析的参数，提取字段的尺寸、精度等信息
     * 默认实现不处理任何参数，具体的DataType实现类可以覆盖此方法来处理特定的DDL参数
     *
     * @param field 基础字段信息（包含字段名等）
     * @param colDataType DDL解析的列数据类型对象
     * @return 包含完整信息的字段对象
     */
    default Field handleDDLParameters(Field field, ColDataType colDataType) {
        // 默认实现只设置基础信息（字段名和类型），具体的DataType实现类可以覆盖此方法来处理特定的DDL参数（如精度、尺寸等）
        Field result = new Field();
        result.setName(field.getName());
        result.setTypeName(field.getTypeName());
        result.setType(field.getType());
        result.setPk(field.isPk());
        return result;
    }
}