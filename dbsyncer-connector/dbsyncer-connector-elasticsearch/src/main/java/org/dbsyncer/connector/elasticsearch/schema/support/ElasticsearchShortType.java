/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ShortType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 短整数类型
 * 支持: short
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchShortType extends ShortType {

    private enum TypeEnum {
        SHORT("short");

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
        if (val instanceof String) {
            return Short.parseShort((String) val);
        }

        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }

        if (val instanceof Boolean) {
            return (short) (((Boolean) val) ? 1 : 0);
        }

        return throwUnsupportedException(val, field);
    }
}
