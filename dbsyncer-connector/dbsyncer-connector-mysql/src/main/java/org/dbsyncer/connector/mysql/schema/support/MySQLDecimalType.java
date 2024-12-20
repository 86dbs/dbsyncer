/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLDecimalType extends DecimalType {

    private enum TypeEnum {
        DECIMAL("DECIMAL"),
        DECIMAL_UNSIGNED("DECIMAL UNSIGNED"),
        BIGINT_UNSIGNED("BIGINT UNSIGNED"),
        BIGINT_UNSIGNED_ZEROFILL("BIGINT UNSIGNED ZEROFILL"),
        NUMERIC("NUMERIC"),
        NUMERIC_UNSIGNED("NUMERIC UNSIGNED"),
        NUMERIC_UNSIGNED_ZEROFILL("NUMERIC UNSIGNED ZEROFILL");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public TypeEnum getTypeEnum(String value) {
            for (TypeEnum type : TypeEnum.values()) {
                if (StringUtil.equals(value, type.value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Can not find type:" + value);
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

    @Override
    protected BigDecimal getDefaultMergedVal() {
        return null;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return val.toString();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}