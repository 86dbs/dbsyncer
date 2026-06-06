/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import com.google.protobuf.ByteString;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;

/**
 * 数据类型解析器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2024-11-25 22:48
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
     * 获取标准字段类型
     */
    DataTypeEnum getFieldType(Field field);

    /**
     * 同步完成的数据，需要做序列化写入磁盘，使用标准的数据类型解析器转换
     *
     * @param value 待序列化的值
     */
    ByteString serialize(Object value);

    /**
     * 反序列化从磁盘读取的数据，使用标准的数据类型解析器转换，返回原始值
     * @param value 序列化的值
     * @param field 数据类型
     */
    Object deserialize(ByteString value, Field field);

}
