/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.IntType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦整型（含 MySQL YEAR/MEDIUMINT 别名）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengIntType extends IntType {

    private enum TypeEnum {
        INT("INT"),
        INTEGER("INTEGER"),
        MEDIUMINT("MEDIUMINT"),
        MEDIUMINT_UNSIGNED("MEDIUMINT UNSIGNED"),
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
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        if (val instanceof Date) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime((Date) val);
            return calendar.get(Calendar.YEAR);
        }
        if (val instanceof String) {
            return Integer.parseInt(((String) val).trim());
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1 : 0;
        }
        return throwUnsupportedException(val, field);
    }
}
