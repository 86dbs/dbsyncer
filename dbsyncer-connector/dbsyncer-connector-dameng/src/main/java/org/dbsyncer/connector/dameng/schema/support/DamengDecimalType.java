/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦精确数值类型（含无符号小数/大整数别名）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengDecimalType extends DecimalType {

    private enum TypeEnum {
        DECIMAL("DECIMAL"),
        NUMERIC("NUMERIC"),
        DEC("DEC"),
        DECIMAL_UNSIGNED("DECIMAL UNSIGNED"),
        BIGINT_UNSIGNED("BIGINT UNSIGNED");

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
        if (val instanceof BigDecimal) {
            return (BigDecimal) val;
        }
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            String str = ((String) val).trim();
            if (str.isEmpty()) {
                return BigDecimal.ZERO;
            }
            try {
                return new BigDecimal(str);
            } catch (NumberFormatException e) {
                return throwUnsupportedException(val, field);
            }
        }
        if (val instanceof Boolean) {
            return new BigDecimal(((Boolean) val) ? 1 : 0);
        }
        return throwUnsupportedException(val, field);
    }
}
