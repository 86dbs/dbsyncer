package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

/**
 * 无符号整型 (0-4294967295)
 * 使用Long存储以避免溢出
 */
public abstract class UnsignedIntType extends AbstractDataType<Long> {

    protected UnsignedIntType() {
        super(Long.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.UNSIGNED_INT;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            // 确保值在 0-4294967295 范围内
            long longVal = num.longValue();
            if (longVal < 0) {
                longVal = 0;
            } else if (longVal > 4294967295L) {
                longVal = 4294967295L;
            }
            return longVal;
        }
        if (val instanceof String) {
            long longVal = NumberUtil.toLong((String) val);
            if (longVal < 0) {
                longVal = 0;
            } else if (longVal > 4294967295L) {
                longVal = 4294967295L;
            }
            return longVal;
        }
        return throwUnsupportedException(val, field);
    }
}

