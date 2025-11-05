package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.JsonType;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL JSON类型支持
 */
public final class PostgreSQLJsonType extends JsonType {

    private enum TypeEnum {
        JSON("json"),
        JSONB("jsonb");

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
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof PGobject) {
            PGobject pgObject = (PGobject) val;
            return pgObject.getValue();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            try {
                PGobject json = new PGobject();
                json.setType(field.getTypeName().toLowerCase());
                json.setValue((String) val);
                return json;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return super.convert(val, field);
    }
}