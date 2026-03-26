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
}
