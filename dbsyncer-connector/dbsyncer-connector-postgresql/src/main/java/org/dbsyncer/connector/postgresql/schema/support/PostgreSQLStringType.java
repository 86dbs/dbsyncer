/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;
import org.postgresql.geometric.PGpoint;
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
        JSONB("jsonb"),
        CHAR("char"),
        BPCHAR("bpchar"),
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
        if (val instanceof PGpoint) {
            PGpoint pgPoint = (PGpoint) val;
            return String.format("POINT (%f %f)", pgPoint.x, pgPoint.y);
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
            TypeEnum typeEnum = TypeEnum.valueOf(field.getTypeName().toUpperCase());
            switch (typeEnum){
                case UUID:
                    return UUIDUtil.fromString((String) val);
                case JSON:
                case JSONB:
                    try {
                        PGobject json = new PGobject();
                        json.setType(typeEnum.getValue());
                        json.setValue((String) val);
                        return json;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                case POINT:
                    return toPoint((String) val);
                default:
                    return val;
            }
        }
        return super.convert(val, field);
    }

    private Object toPoint(String val) {
        try {
            PGpoint pgPoint = new PGpoint();
            WKTReader reader = new WKTReader();
            Geometry geom = reader.read(val);
            if (geom instanceof Point) {
                Point point = (Point) geom;
                pgPoint.x = point.getX();
                pgPoint.y = point.getY();
            }
            return pgPoint;
        } catch (ParseException e) {
            throw new PostgreSQLException(e);
        }
    }
}