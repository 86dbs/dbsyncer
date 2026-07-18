/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦长整型（含 MySQL INT UNSIGNED 别名）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengLongType extends LongType {

    private enum TypeEnum {
        BIGINT("BIGINT"),
        INT_UNSIGNED("INT UNSIGNED");

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
    protected Long merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof String) {
            return Long.parseLong(((String) val).trim());
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        return throwUnsupportedException(val, field);
    }
}
