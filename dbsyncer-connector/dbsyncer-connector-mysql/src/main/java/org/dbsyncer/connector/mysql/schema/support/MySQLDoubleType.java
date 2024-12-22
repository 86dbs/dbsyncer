/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLDoubleType extends DoubleType {

    private enum TypeEnum {
        DOUBLE("DOUBLE"),
        DOUBLE_UNSIGNED("DOUBLE UNSIGNED"),
        DOUBLE_UNSIGNED_ZEROFILL("DOUBLE UNSIGNED ZEROFILL"),
        DOUBLE_PRECISION("DOUBLE PRECISION"),
        DOUBLE_PRECISION_UNSIGNED("DOUBLE PRECISION UNSIGNED"),
        DOUBLE_PRECISION_UNSIGNED_ZEROFILL("DOUBLE PRECISION UNSIGNED ZEROFILL"),
        REAL("REAL"),
        REAL_UNSIGNED("REAL UNSIGNED"),
        REAL_UNSIGNED_ZEROFILL("REAL UNSIGNED ZEROFILL");

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
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Double getDefaultMergedVal() {
        return null;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}