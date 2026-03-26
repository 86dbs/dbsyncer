/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ES 二进制类型
 * 支持: binary
 * ES 中 binary 类型以 Base64 编码存储
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class ElasticsearchBytesType extends BytesType {

    private enum TypeEnum {

        BINARY("binary");

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
    protected byte[] merge(Object val, Field field) {
        if (val instanceof String) {
            // The binary type accepts a binary value as a Base64 encoded string. The field is not
            // stored by default and is not searchable.
            // ES binary 类型以 Base64 编码存储
            try {
                return Base64.getDecoder().decode((String) val);
            } catch (IllegalArgumentException e) {
                // 如果不是 Base64 编码，直接转为字节数组
                return ((String) val).getBytes(StandardCharsets.UTF_8);
            }
        }

        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            // 转换为 Base64 编码的字符串，供 ES 存储
            return Base64.getEncoder().encodeToString((byte[]) val);
        }
        return super.convert(val, field);
    }
}
