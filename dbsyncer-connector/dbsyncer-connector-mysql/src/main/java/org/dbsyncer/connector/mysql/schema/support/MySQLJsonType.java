package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.JsonType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL JSON类型支持
 */
public final class MySQLJsonType extends JsonType {

    private enum TypeEnum {
        JSON
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
        return throwUnsupportedException(val, field);
    }
}