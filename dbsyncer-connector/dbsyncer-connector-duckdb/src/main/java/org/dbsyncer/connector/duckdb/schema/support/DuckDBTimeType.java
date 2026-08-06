/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimeType;

import java.sql.Time;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DuckDB TIME
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 11:25
 */
public final class DuckDBTimeType extends TimeType {

    private enum TypeEnum {
        TIME("TIME");

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
    protected Time merge(Object val, Field field) {
        if (val instanceof Time) {
            return (Time) val;
        }
        if (val instanceof String) {
            String text = ((String) val).trim();
            int dot = text.indexOf('.');
            if (dot > 0) {
                text = text.substring(0, dot);
            }
            return Time.valueOf(text);
        }
        return throwUnsupportedException(val, field);
    }
}
