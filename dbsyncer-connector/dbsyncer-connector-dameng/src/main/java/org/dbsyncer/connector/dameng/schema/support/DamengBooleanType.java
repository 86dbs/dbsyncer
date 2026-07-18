/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦布尔类型
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengBooleanType extends BooleanType {

    private enum TypeEnum {
        BOOLEAN
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Boolean merge(Object val, Field field) {
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        if (val instanceof Number) {
            return ((Number) val).intValue() != 0;
        }
        if (val instanceof String) {
            String text = ((String) val).trim();
            if ("1".equals(text) || "true".equalsIgnoreCase(text) || "Y".equalsIgnoreCase(text)) {
                return Boolean.TRUE;
            }
            if ("0".equals(text) || "false".equalsIgnoreCase(text) || "N".equalsIgnoreCase(text)) {
                return Boolean.FALSE;
            }
        }
        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            return bytes.length > 0 && bytes[0] != 0;
        }
        return throwUnsupportedException(val, field);
    }
}
