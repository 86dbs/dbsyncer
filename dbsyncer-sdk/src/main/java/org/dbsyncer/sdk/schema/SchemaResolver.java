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
     * 将数据库特定类型转换为标准类型
     * 用于字段映射配置阶段的类型标准化，不需要实际数据值
     *
     * @param field 源字段（包含数据库特定类型信息）
     * @return 标准化后的字段
     */
    Field toStandardType(Field field);
    
    /**
     * 从标准类型转换为目标数据库类型
     * 用于字段映射配置阶段的类型转换
     *
     * @param standardField 标准类型字段
     * @return 目标数据库类型字段
     */
    Field fromStandardType(Field standardField);
}