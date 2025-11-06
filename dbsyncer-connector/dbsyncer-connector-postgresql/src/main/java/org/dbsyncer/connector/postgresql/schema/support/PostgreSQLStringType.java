package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeStringType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * PostgreSQL字符串类型支持
 * <p>
 * PostgreSQL的VARCHAR/CHAR类型默认支持UTF-8，
 * 因此标准化为UNICODE_STRING以确保数据安全性和跨数据库兼容性。
 * </p>
 */
public final class PostgreSQLStringType extends UnicodeStringType {
    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("VARCHAR", "CHAR", "BPCHAR"));
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
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