/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.connector.mysql.MySQLException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:24
 */
public final class MySQLStringType extends StringType {

    private enum TypeEnum {
        CHAR, // 固定长度，最多255个字符
        VARCHAR, // 固定长度，最多65535个字符，64K
        TINYTEXT, // 可变长度，最多255字符
        TEXT, // 可变长度，最多65535个字符，64K
        MEDIUMTEXT, // 可变长度，最多2的24次方-1个字符，16M
        LONGTEXT, // 可变长度，最多2的32次方-1个字符，4GB
        ENUM, // 2字节，最大可达65535个不同的枚举值
        JSON, GEOMETRY; // POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        switch (TypeEnum.valueOf(field.getTypeName())) {
            case GEOMETRY:
                return deserializeGeometry((byte[]) val);
            case ENUM:
                return String.valueOf(val);
            default:
               break;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
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
            throw new MySQLException(e);
        }
    }

    private byte[] serializeGeometry(String wellKnownText) {
        try {
            Geometry geometry = new WKTReader().read(wellKnownText);
            byte[] bytes = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geometry);
            return ByteBuffer.allocate(bytes.length + 4).order(ByteOrder.LITTLE_ENDIAN).putInt(geometry.getSRID()).put(bytes).array();
        } catch (ParseException e) {
            throw new MySQLException(e);
        }
    }
}