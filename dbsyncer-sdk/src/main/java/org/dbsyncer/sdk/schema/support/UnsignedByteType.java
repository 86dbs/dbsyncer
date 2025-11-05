package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

/**
 * 无符号字节类型 (0-255)
 * 使用Short存储以避免溢出
 */
public abstract class UnsignedByteType extends AbstractDataType<Short> {

    protected UnsignedByteType() {
        super(Short.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNSIGNED_BYTE;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            // 确保值在 0-255 范围内
            int intVal = num.intValue();
            if (intVal < 0) {
                intVal = 0;
            } else if (intVal > 255) {
                intVal = 255;
            }
            return (short) intVal;
        }
        if (val instanceof String) {
            int intVal = NumberUtil.toInt((String) val);
            if (intVal < 0) {
                intVal = 0;
            } else if (intVal > 255) {
                intVal = 255;
            }
            return (short) intVal;
        }
        return throwUnsupportedException(val, field);
    }
}

