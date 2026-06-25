/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DateType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseDateType extends DateType {

    private enum TypeEnum {
        DATE("date"),
        DATE32("date32");

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
    protected Date merge(Object val, Field field) {
        if (val instanceof Date) {
            return (Date) val;
        }
        if (val instanceof LocalDate) {
            return Date.valueOf((LocalDate) val);
        }
        if (val instanceof Timestamp) {
            return new Date(((Timestamp) val).getTime());
        }
        if (val instanceof java.util.Date) {
            return new Date(((java.util.Date) val).getTime());
        }
        if (val instanceof LocalDateTime) {
            return new Date(Timestamp.valueOf((LocalDateTime) val).getTime());
        }
        if (val instanceof String) {
            String s = ((String) val).trim();
            if (isZeroDate(s)) {
                return null;
            }
            Timestamp timestamp = DateFormatUtil.stringToTimestamp(s);
            if (timestamp != null) {
                return new Date(timestamp.getTime());
            }
            return DateFormatUtil.stringToDate(s);
        }
        return throwUnsupportedException(val, field);
    }

    private boolean isZeroDate(String value) {
        return StringUtil.equals(value, "0000-00-00")
                || StringUtil.equals(value, "0000-00-00 00:00:00");
    }
}
