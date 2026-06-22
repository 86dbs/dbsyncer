/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.h2.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * H2 浮点型
 */
public final class H2DoubleType extends DoubleType {

    private enum TypeEnum {

        DOUBLE("DOUBLE"),
        DOUBLE_PRECISION("DOUBLE PRECISION"),
        REAL("REAL"),
        FLOAT("FLOAT");

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
    protected Double merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }
}
