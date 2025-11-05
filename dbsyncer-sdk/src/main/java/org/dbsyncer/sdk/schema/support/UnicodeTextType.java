package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;

/**
 * Unicode大文本类型
 * 继承自TextType的所有功能，但返回UNICODE_TEXT类型枚举
 */
public abstract class UnicodeTextType extends TextType {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNICODE_TEXT;
    }

}

