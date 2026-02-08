/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.enums;

import org.dbsyncer.sdk.SdkException;

/**
 * 标准数据类型
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-23 22:39
 */
public enum DataTypeEnum {

    /** 文本 */
    STRING,
    /** 整型 */
    BYTE, SHORT, INT, LONG,
    /** 浮点型 */
    DECIMAL, DOUBLE, FLOAT,
    /** 布尔型 */
    BOOLEAN,
    /** 时间 */
    DATE, TIME, TIMESTAMP,
    /** 二进制 */
    BYTES;

    public static DataTypeEnum getType(String type) {
        for (DataTypeEnum e : DataTypeEnum.values()) {
            if (e.name().equals(type)) {
                return e;
            }
        }
        throw new SdkException(String.format("DataTypeEnum type \"%s\" does not exist.", type));
    }
}
