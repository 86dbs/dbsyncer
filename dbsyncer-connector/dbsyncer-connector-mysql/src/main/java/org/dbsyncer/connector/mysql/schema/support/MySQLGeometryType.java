package org.dbsyncer.connector.mysql.schema.support;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.*;
import org.dbsyncer.connector.mysql.MySQLException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL GEOMETRY类型支持
 *
 */
public final class MySQLGeometryType extends StringType {

    private enum TypeEnum {
        GEOMETRY,
        POINT,
        LINESTRING,
        POLYGON,
        MULTIPOINT,
        MULTILINESTRING,
        MULTIPOLYGON,
        GEOMETRYCOLLECTION
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return deserializeGeometry((byte[]) val);
        }
        // 对于其他类型，使用父类的默认实现
        return super.merge(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return serializeGeometry((String) val);
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