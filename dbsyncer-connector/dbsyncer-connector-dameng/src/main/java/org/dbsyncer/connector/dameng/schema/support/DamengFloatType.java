/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.FloatType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦单精度浮点（REAL）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengFloatType extends FloatType {

    private enum TypeEnum {
        REAL
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Float merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).floatValue();
        }
        if (val instanceof String) {
            return Float.parseFloat(((String) val).trim());
        }
        return throwUnsupportedException(val, field);
    }
}
