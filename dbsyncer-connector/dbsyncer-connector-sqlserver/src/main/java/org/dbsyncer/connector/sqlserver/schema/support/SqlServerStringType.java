/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class SqlServerStringType extends StringType {

    private enum TypeEnum {
        CHAR("char"), VARCHAR("varchar"), NVARCHAR("nvarchar"), TEXT("text"), GEOMETRY("geometry");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static TypeEnum getType(String type) {
            for (TypeEnum e : TypeEnum.values()) {
                if (e.getValue().equals(type)) {
                    return e;
                }
            }
            return null;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            if (TypeEnum.getType(field.getTypeName()) == TypeEnum.GEOMETRY) {
                return deserializeGeometry((byte[]) val);
            }
            return new String((byte[]) val);
        }

        if (val instanceof Number) {
            Number number = (Number) val;
            return number.toString();
        }

        if (val instanceof Timestamp) {
            return DateFormatUtil.timestampToString((Timestamp) val);
        }

        if (val instanceof Date) {
            return DateFormatUtil.dateToString((Date) val);
        }

        if (val instanceof java.util.Date) {
            return DateFormatUtil.dateToString((java.util.Date) val);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            if (TypeEnum.valueOf(field.getTypeName()) == TypeEnum.GEOMETRY) {
                return serializeGeometry((String) val);
            }
            return val;
        }
        return super.convert(val, field);
    }

    private String deserializeGeometry(byte[] bytes) {
        try {
            byte[] geometryBytes = ByteBuffer.allocate(bytes.length - 4).order(ByteOrder.LITTLE_ENDIAN).put(bytes, 4, bytes.length - 4).array();
            WKBReader reader = new WKBReader();
            return reader.read(geometryBytes).toText();
        } catch (ParseException e) {
            throw new SqlServerException(e);
        }
    }

    private byte[] serializeGeometry(String wellKnownText) {
        try {
            Geometry geometry = new WKTReader().read(wellKnownText);
            byte[] bytes = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geometry);
            return ByteBuffer.allocate(bytes.length + 4).order(ByteOrder.LITTLE_ENDIAN).putInt(geometry.getSRID()).put(bytes).array();
        } catch (ParseException e) {
            throw new SqlServerException(e);
        }
    }

}
