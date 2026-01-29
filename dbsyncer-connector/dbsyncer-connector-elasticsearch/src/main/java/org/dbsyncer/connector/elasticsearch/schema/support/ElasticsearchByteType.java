/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ByteType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 字节类型
 * 支持: byte
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchByteType extends ByteType {

    private enum TypeEnum {
        BYTE("byte");

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
    protected Byte merge(Object val, Field field) {
        if (val instanceof String) {
            return Byte.parseByte((String) val);
        }

        if (val instanceof Number) {
            return ((Number) val).byteValue();
        }

        if (val instanceof Boolean) {
            return (byte) (((Boolean) val) ? 1 : 0);
        }

        return throwUnsupportedException(val, field);
    }
}
