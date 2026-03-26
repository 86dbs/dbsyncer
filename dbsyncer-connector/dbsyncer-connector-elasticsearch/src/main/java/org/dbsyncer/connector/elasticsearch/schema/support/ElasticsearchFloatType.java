/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.FloatType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 浮点类型
 * 支持: float, half_float
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchFloatType extends FloatType {

    private enum TypeEnum {

        FLOAT("float"), HALF_FLOAT("half_float");

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
    protected Float merge(Object val, Field field) {
        if (val instanceof String) {
            return Float.parseFloat((String) val);
        }

        if (val instanceof Number) {
            return ((Number) val).floatValue();
        }

        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1.0f : 0.0f;
        }

        return throwUnsupportedException(val, field);
    }
}
