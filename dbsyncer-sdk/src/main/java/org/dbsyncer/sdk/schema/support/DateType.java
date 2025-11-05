package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public abstract class DateType extends AbstractDataType<Date> {

    protected DateType() {
        super(Date.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.DATE;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Date) {
            return val;
        }

        if (val instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) val;
            return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
        }

        if (val instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) val;
            return Timestamp.valueOf(dateTime);
        }

        if (val instanceof String) {
            String s = (String) val;
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (null != timestamp) {
                return Date.valueOf(timestamp.toLocalDateTime().toLocalDate());
            }
        }
        return throwUnsupportedException(val, field);
    }
}