/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimeType;

import java.sql.Time;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLTimeType extends TimeType {

    private enum TypeEnum {
        TIME;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Time merge(Object val, Field field) {
        if (val instanceof Time) {
            return (Time) val;
        }
        if (val instanceof String) {
            return parseTimeString((String) val);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Time) {
            return val;
        }
        if (val instanceof String) {
            return parseTimeString((String) val);
        }
        return throwUnsupportedException(val, field);
    }

    private Time parseTimeString(String value) {
        String text = value.trim();
        int dot = text.indexOf('.');
        if (dot > 0) {
            text = text.substring(0, dot);
        }
        return Time.valueOf(text);
    }
}
