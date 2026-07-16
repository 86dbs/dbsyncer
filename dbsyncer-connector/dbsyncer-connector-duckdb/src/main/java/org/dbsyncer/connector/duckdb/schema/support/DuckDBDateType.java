/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DateType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DuckDB DATE
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 11:25
 */
public final class DuckDBDateType extends DateType {

    private enum TypeEnum {
        DATE("DATE");

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
        return throwUnsupportedException(val, field);
    }
}
