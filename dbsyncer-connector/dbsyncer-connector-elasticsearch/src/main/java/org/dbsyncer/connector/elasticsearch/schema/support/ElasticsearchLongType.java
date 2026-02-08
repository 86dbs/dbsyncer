/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 长整数类型
 * 支持: long, token_count
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchLongType extends LongType {

    private enum TypeEnum {

        LONG("long"), TOKEN_COUNT("token_count");

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
        if (val instanceof String) {
            return Long.parseLong((String) val);
        }

        if (val instanceof Number) {
            return ((Number) val).longValue();
        }

        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }

        if (val instanceof Timestamp) {
            return ((Timestamp) val).getTime();
        }

        if (val instanceof Date) {
            return ((Date) val).getTime();
        }

        if (val instanceof java.util.Date) {
            return ((java.util.Date) val).getTime();
        }

        return throwUnsupportedException(val, field);
    }
}
