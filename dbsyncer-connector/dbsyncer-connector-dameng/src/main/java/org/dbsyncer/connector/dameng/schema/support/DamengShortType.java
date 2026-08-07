/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ShortType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦短整型（含 MySQL 无符号小整型别名）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengShortType extends ShortType {

    private enum TypeEnum {
        SMALLINT("SMALLINT"),
        TINYINT_UNSIGNED("TINYINT UNSIGNED"),
        SMALLINT_UNSIGNED("SMALLINT UNSIGNED");

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
    protected Short merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        if (val instanceof String) {
            return Short.parseShort(((String) val).trim());
        }
        if (val instanceof Boolean) {
            return (short) (((Boolean) val) ? 1 : 0);
        }
        return throwUnsupportedException(val, field);
    }
}
