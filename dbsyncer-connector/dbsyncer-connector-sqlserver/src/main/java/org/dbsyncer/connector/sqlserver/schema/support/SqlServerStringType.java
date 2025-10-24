/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server 字符字符串类型
 * 包括非 Unicode 字符类型
 * 注意：字符字符串类型不支持 IDENTITY 属性
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SqlServerStringType extends StringType {

    private enum TypeEnum {
        CHAR,           // 固定长度字符
        VARCHAR,        // 可变长度字符
        TEXT,           // 文本类型 (已弃用，建议使用 VARCHAR(MAX))
        NCHAR,          // 固定长度 Unicode 字符
        NVARCHAR,       // 可变长度 Unicode 字符
        NTEXT,          // Unicode 文本类型 (已弃用，建议使用 NVARCHAR(MAX))
        XML
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }
}

