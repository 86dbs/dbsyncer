package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.GeometryType;
import org.postgis.Geometry;
import org.postgis.binary.BinaryParser;
import org.postgis.binary.BinaryWriter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * PostgreSQL GEOMETRY类型支持
 * PostgreSQL的Geometry类型使用PostGIS扩展，以HexEWKB字符串格式存储
 * HexEWKB是扩展的WKB格式，包含SRID信息
 */
public final class PostgreSQLGeometryType extends GeometryType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("GEOMETRY", "GEOGRAPHY", "POINT", "LINESTRING", "POLYGON", 
                "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        // GeometryType 重写了 mergeValue 方法，确保即使 val 是 byte[] 类型，也会调用此 merge 方法
        // PostgreSQL从JDBC读取Geometry数据时，通常返回String（HexEWKB）或PGgeometry对象
        // 但如果通过getBytes()方法获取，会返回byte[]格式的EWKB数据
        if (val instanceof byte[]) {
            // PostgreSQL从JDBC读取Geometry数据时，通常返回String（HexEWKB）或PGgeometry对象
            // 如果收到byte[]，可能是通过getBytes()方法获取的原始二进制数据
            // PostgreSQL的Geometry二进制格式是EWKB（Extended WKB），包含SRID信息
            // 需要将EWKB转换为标准WKB格式
            try {
                // 将byte[]转换为Hex字符串，然后解析为Geometry对象
                // EWKB格式：标准WKB + SRID（4字节，大端序）
                BinaryParser parser = new BinaryParser();
                // 需要将byte[]转换为Hex字符串格式
                String hexString = bytesToHex((byte[]) val);
                Geometry geo = parser.parse(hexString);
                
                // 提取SRID
                int srid = geo.getSrid();
                field.setSrid(srid);
                
                // 转换为标准WKB格式（不带SRID）
                BinaryWriter bw = new BinaryWriter();
                return bw.writeBinary(geo);
            } catch (Exception e) {
                throw new PostgreSQLException("Failed to parse PostgreSQL geometry from byte array: " + e.getMessage(), e);
            }
        }
        if (val instanceof String) {
            // PostgreSQL的Geometry以HexEWKB字符串格式存储
            // HexEWKB格式：扩展的WKB，包含SRID信息
            try {
                BinaryParser parser = new BinaryParser();
                Geometry geo = parser.parse((String) val);
                
                // 提取SRID
                int srid = geo.getSrid();
                field.setSrid(srid);
                
                // 转换为标准WKB格式（不带SRID）
                BinaryWriter bw = new BinaryWriter();
                // writeBinary方法会写入标准WKB格式（不包含SRID）
                return bw.writeBinary(geo);
            } catch (Exception e) {
                throw new PostgreSQLException("Failed to parse PostgreSQL geometry: " + e.getMessage(), e);
            }
        }
        // 处理PostGIS的PGgeometry对象（如果从JDBC直接获取）
        if (val instanceof org.postgis.PGgeometry) {
            try {
                org.postgis.PGgeometry pgGeometry = (org.postgis.PGgeometry) val;
                Geometry geo = pgGeometry.getGeometry();
                
                // 提取SRID
                int srid = geo.getSrid();
                field.setSrid(srid);
                
                // 转换为标准WKB格式
                BinaryWriter bw = new BinaryWriter();
                return bw.writeBinary(geo);
            } catch (Exception e) {
                throw new PostgreSQLException("Failed to convert PGgeometry to WKB: " + e.getMessage(), e);
            }
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            // 标准WKB格式转换为PostgreSQL的HexEWKB格式
            // 注意：实际写入时，PostgreSQL的ValueMapper会处理byte[]到HexEWKB字符串的转换
            // 这里我们只需要确保byte[]是标准WKB格式即可
            // SRID信息会通过Field传递，由ValueMapper在写入时添加到HexEWKB中
            return val;
        }
        return throwUnsupportedException(val, field);
    }

    /**
     * 将byte[]转换为十六进制字符串
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}

