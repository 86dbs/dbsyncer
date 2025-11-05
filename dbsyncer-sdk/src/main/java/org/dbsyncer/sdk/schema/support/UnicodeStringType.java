package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;

/**
 * Unicode字符串类型
 * 继承自StringType的所有功能，但返回UNICODE_STRING类型枚举
 */
public abstract class UnicodeStringType extends StringType {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNICODE_STRING;
    }

}

