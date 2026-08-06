/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DuckDB DECIMAL / NUMERIC
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 11:25
 */
public final class DuckDBDecimalType extends DecimalType {

    private enum TypeEnum {
        DECIMAL("DECIMAL"),
        NUMERIC("NUMERIC");

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
    protected BigDecimal merge(Object val, Field field) {
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        return throwUnsupportedException(val, field);
    }
}
