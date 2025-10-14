/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.model.Field;

/**
 * 数据类型解析器
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-25 22:48
 */
public interface SchemaResolver {

    /**
     * 转换为标准数据类型
     *
     * @param val   转换值
     * @param field 数据类型
     * @return Object
     */
    Object merge(Object val, Field field);

    /**
     * 转换为指定数据类型
     *
     * @param val   转换值
     * @param field 数据类型
     * @return Object
     */
    Object convert(Object val, Field field);

    /**
     * 将数据库特定类型转换为标准中间类型
     * 用于字段映射配置阶段的类型标准化，不需要实际数据值
     *
     * @param field 源字段（包含数据库特定类型信息）
     * @return 标准化后的字段
     */
    default Field toMiddleType(Field field) {
        // 默认实现：直接返回字段的副本
        return new Field(field.getName(), field.getTypeName(), field.getType(), 
                        field.isPk(), field.getColumnSize(), field.getRatio());
    }

    /**
     * 将标准中间类型转换为目标数据库类型
     * 用于字段映射配置阶段的类型转换
     *
     * @param middleField 标准中间类型字段
     * @param targetField 目标字段（可选，用于参考目标数据库的约束）
     * @return 目标数据库的字段
     */
    default Field toTargetType(Field middleField, Field targetField) {
        // 默认实现：保持中间类型不变
        return new Field(middleField.getName(), middleField.getTypeName(), middleField.getType(),
                        middleField.isPk(), middleField.getColumnSize(), middleField.getRatio());
    }

}