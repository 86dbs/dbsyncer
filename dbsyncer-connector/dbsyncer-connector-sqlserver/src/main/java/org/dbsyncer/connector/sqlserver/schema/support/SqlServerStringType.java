package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server字符串类型支持
 */
public final class SqlServerStringType extends StringType {

    private enum TypeEnum {
        CHAR,           // 固定长度字符
        VARCHAR,        // 可变长度字符
        NCHAR,          // 固定长度 Unicode 字符
        NVARCHAR        // 可变长度 Unicode 字符
        // 移除了TEXT、NTEXT、XML，因为它们有专门的DataType实现类
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
            // 使用UTF-8编码将字节数组转换为字符串
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        // 对于其他类型，直接转换为字符串
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        return super.convert(val, field);
    }
    
}