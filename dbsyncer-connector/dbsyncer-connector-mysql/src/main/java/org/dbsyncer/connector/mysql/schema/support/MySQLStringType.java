package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeStringType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL字符串类型支持
 * <p>
 * MySQL的VARCHAR/CHAR类型默认支持UTF-8（通过字符集配置），
 * 因此标准化为UNICODE_STRING以确保数据安全性和跨数据库兼容性。
 * </p>
 */
public final class MySQLStringType extends UnicodeStringType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("CHAR", "VARCHAR"));
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

    @Override
    protected Boolean determineIsSizeFixed(String typeName) {
        if (typeName == null) {
            return null;
        }
        
        String upperTypeName = typeName.toUpperCase();
        
        // MySQL固定长度类型：CHAR
        if ("CHAR".equals(upperTypeName)) {
            return true;
        }
        
        // MySQL可变长度类型：VARCHAR
        if ("VARCHAR".equals(upperTypeName)) {
            return false;
        }
        
        return null;
    }

}