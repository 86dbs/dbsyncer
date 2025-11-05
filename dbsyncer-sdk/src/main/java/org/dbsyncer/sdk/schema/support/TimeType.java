package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.sql.Time;

public abstract class TimeType extends AbstractDataType<Time> {

    protected TimeType() {
        super(Time.class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.TIME;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Time) {
            return val;
        }
        return throwUnsupportedException(val, field);
    }
}