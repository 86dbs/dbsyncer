/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:04
 */
public class PostgreSQLStringType extends StringType {
    private enum TypeEnum {
        UUID("uuid"),
        VARCHAR("varchar"),
        TEXT("text"),
        JSON("json"),
        POINT("point");

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
        if (val instanceof UUID) {
            return val.toString();
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
            switch (TypeEnum.valueOf(field.getTypeName().toUpperCase())){
                case UUID:
                    return UUIDUtil.fromString((String) val);
                case JSON:
                    try {
                        PGobject json = new PGobject();
                        json.setType(TypeEnum.JSON.getValue());
                        json.setValue((String) val);
                        return json;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                default:
                    return val;
            }
        }
        return super.convert(val, field);
    }
}