package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeStringType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL字符串类型支持
 * <p>
 * MySQL的VARCHAR/CHAR类型默认支持UTF-8（通过字符集配置），
 * 因此标准化为UNICODE_STRING以确保数据安全性和跨数据库兼容性。
 * </p>
 */
public final class MySQLStringType extends UnicodeStringType {

    private enum TypeEnum {
        CHAR, // 固定长度，最多255个字符
        VARCHAR; // 固定长度，最多65535个字符，64K
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }

}