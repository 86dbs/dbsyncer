package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public abstract class StringType extends AbstractDataType<String> {

    protected StringType() {
        super(String.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.STRING;
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        // 对于其他类型，转换为字符串
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }

        if (val instanceof Number) {
            Number number = (Number) val;
            return number.toString();
        }

        if (val instanceof LocalDateTime) {
            return ((LocalDateTime) val).format(DateFormatUtil.YYYY_MM_DD_HH_MM_SS);
        }

        if (val instanceof LocalDate) {
            return ((LocalDate) val).format(DateFormatUtil.YYYY_MM_DD);
        }

        if (val instanceof Timestamp) {
            return DateFormatUtil.timestampToString((Timestamp) val);
        }

        if (val instanceof Date) {
            return DateFormatUtil.dateToString((Date) val);
        }

        if (val instanceof java.util.Date) {
            return DateFormatUtil.dateToString((java.util.Date) val);
        }

        return throwUnsupportedException(val, field);
    }
}