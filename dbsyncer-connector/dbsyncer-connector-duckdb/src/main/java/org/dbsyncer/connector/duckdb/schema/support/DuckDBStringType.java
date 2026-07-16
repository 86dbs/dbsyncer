/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DuckDB 字符串类型
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 10:00
 */
public final class DuckDBStringType extends StringType {

    private enum TypeEnum {
        VARCHAR("VARCHAR"),
        CHAR("CHAR"),
        STRING("STRING"),
        TEXT("TEXT"),
        JSON("JSON"),
        UUID("UUID");

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
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        if (val instanceof Number) {
            return val.toString();
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
