/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.IntType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLIntType extends IntType {

    private enum TypeEnum {
        SMALLINT_UNSIGNED("SMALLINT UNSIGNED"),
        MEDIUMINT("MEDIUMINT"),
        MEDIUMINT_UNSIGNED("MEDIUMINT UNSIGNED"),
        INT("INT"),
        INTEGER("INTEGER"),
        YEAR("YEAR");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected Integer merge(Object val, Field field) {
        if (val instanceof Date) {
            Date d = (Date) val;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d);
            return calendar.get(Calendar.YEAR);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        return throwUnsupportedException(val, field);
    }

}