package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TextType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL TEXT类型支持
 */
public final class MySQLTextType extends TextType {

    private enum TypeEnum {
        TINYTEXT,
        TEXT,
        MEDIUMTEXT,
        LONGTEXT
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