package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ShortType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL YEAR类型：用于存储年份（1901-2155）
 * 映射到标准类型 SHORT，在 SQL Server 中转换为 SMALLINT
 */
public final class MySQLYearType extends ShortType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("YEAR"));
    }

    @Override
    protected Short merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        if (val instanceof Date) {
            Date d = (Date) val;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d);
            return (short) calendar.get(Calendar.YEAR);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Short) {
            return val;
        }
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        if (val instanceof Date) {
            Date d = (Date) val;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d);
            return (short) calendar.get(Calendar.YEAR);
        }
        return super.convert(val, field);
    }
}

