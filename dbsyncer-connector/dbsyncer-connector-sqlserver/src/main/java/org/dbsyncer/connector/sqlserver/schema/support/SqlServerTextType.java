package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TextType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server TEXT类型支持
 */
public final class SqlServerTextType extends TextType {

    private enum TypeEnum {
        TEXT,
        NTEXT,
        VARCHAR,
        NVARCHAR
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
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        return throwUnsupportedException(val, field);
    }
}