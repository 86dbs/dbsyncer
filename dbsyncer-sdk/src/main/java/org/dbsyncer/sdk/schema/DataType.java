/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.sdk.enums.DataTypeEnum;

import java.io.Serializable;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-23 23:00
 */
public interface DataType extends Serializable {

    /**
     * 转换为标准数据类型
     *
     * @param dataType 转换类型
     * @param val 转换值
     * @return Object
     */
    Object convert(DataType dataType, Object val);

    /**
     * 返回数据类型
     *
     * @return
     */
    DataTypeEnum getType();
}
