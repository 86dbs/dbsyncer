package org.dbsyncer.connector.sqlserver.schema.support;

import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.GeometryType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server GEOMETRY类型支持
 * SQL Server的Geometry类型使用自定义二进制格式
 * 需要通过WKT作为中间格式转换为标准WKB
 */
public final class SqlServerGeometryType extends GeometryType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("GEOMETRY", "GEOGRAPHY"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            // SQL Server的Geometry数据是自定义二进制格式
            // 需要通过SQL Server的Geometry类转换为WKT，再转换为标准WKB
            try {
                com.microsoft.sqlserver.jdbc.Geometry sqlServerGeometry = com.microsoft.sqlserver.jdbc.Geometry.deserialize((byte[]) val);
                
                // 提取SRID
                int srid = sqlServerGeometry.getSrid();
                field.setSrid(srid);
                
                // 转换为WKT格式
                String wkt = sqlServerGeometry.toString();
                
                // 从WKT转换为标准WKB
                WKTReader wktReader = new WKTReader();
                com.vividsolutions.jts.geom.Geometry jtsGeometry = wktReader.read(wkt);
                jtsGeometry.setSRID(srid);
                
                WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
                return wkbWriter.write(jtsGeometry);
            } catch (Exception e) {
                throw new SqlServerException("Failed to convert SQL Server geometry to WKB: " + e.getMessage(), e);
            }
        }
        if (val instanceof com.microsoft.sqlserver.jdbc.Geometry) {
            // 如果是从JDBC获取的Geometry对象
            try {
                com.microsoft.sqlserver.jdbc.Geometry sqlServerGeometry = (com.microsoft.sqlserver.jdbc.Geometry) val;
                
                // 提取SRID
                int srid = sqlServerGeometry.getSrid();
                field.setSrid(srid);
                
                // 转换为WKT格式
                String wkt = sqlServerGeometry.toString();
                
                // 从WKT转换为标准WKB
                WKTReader wktReader = new WKTReader();
                com.vividsolutions.jts.geom.Geometry jtsGeometry = wktReader.read(wkt);
                jtsGeometry.setSRID(srid);
                
                WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
                return wkbWriter.write(jtsGeometry);
            } catch (Exception e) {
                throw new SqlServerException("Failed to convert SQL Server Geometry object to WKB: " + e.getMessage(), e);
            }
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            // 标准WKB格式转换为SQL Server的二进制格式
            // 需要通过WKT作为中间格式
            try {
                // 从标准WKB转换为WKT
                WKBReader wkbReader = new WKBReader();
                com.vividsolutions.jts.geom.Geometry jtsGeometry = wkbReader.read((byte[]) val);
                
                // 获取SRID（如果未设置，使用默认值0）
                Integer srid = field.getSrid();
                if (srid != null) {
                    jtsGeometry.setSRID(srid);
                } else {
                    jtsGeometry.setSRID(0); // SQL Server默认SRID为0
                }
                
                // 转换为WKT格式
                String wkt = jtsGeometry.toText();
                
                // 从WKT创建SQL Server的Geometry对象，然后序列化
                com.microsoft.sqlserver.jdbc.Geometry sqlServerGeometry = com.microsoft.sqlserver.jdbc.Geometry.STGeomFromText(wkt, srid != null ? srid : 0);
                return sqlServerGeometry.serialize();
            } catch (Exception e) {
                throw new SqlServerException("Failed to convert WKB to SQL Server geometry: " + e.getMessage(), e);
            }
        }
        return throwUnsupportedException(val, field);
    }
}

