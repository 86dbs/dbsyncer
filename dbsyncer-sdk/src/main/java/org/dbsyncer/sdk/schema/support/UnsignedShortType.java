package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

/**
 * 无符号短整型 (0-65535)
 * 使用Integer存储以避免溢出
 */
public abstract class UnsignedShortType extends AbstractDataType<Integer> {

    protected UnsignedShortType() {
        super(Integer.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNSIGNED_SHORT;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            // 确保值在 0-65535 范围内
            int intVal = num.intValue();
            if (intVal < 0) {
                intVal = 0;
            } else if (intVal > 65535) {
                intVal = 65535;
            }
            return intVal;
        }
        if (val instanceof String) {
            int intVal = NumberUtil.toInt((String) val);
            if (intVal < 0) {
                intVal = 0;
            } else if (intVal > 65535) {
                intVal = 65535;
            }
            return intVal;
        }
        return throwUnsupportedException(val, field);
    }
}

