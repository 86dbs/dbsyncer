package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.GeometryType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL GEOMETRY类型支持
 * MySQL的Geometry类型以WKB格式存储（带SRID前缀）
 * 格式：[SRID(4字节小端序)] + [WKB数据]
 */
public final class MySQLGeometryType extends GeometryType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            // MySQL的Geometry数据格式：[SRID(4字节)] + [WKB数据]
            if (bytes.length >= 4) {
                // 提取SRID（前4字节，小端序）
                ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN);
                int srid = buffer.getInt();
                field.setSrid(srid);
                
                // 提取WKB数据（从第5字节开始）
                byte[] wkbData = new byte[bytes.length - 4];
                System.arraycopy(bytes, 4, wkbData, 0, wkbData.length);
                
                // 返回标准WKB格式（不带SRID）
                return wkbData;
            }
            // 如果数据长度不足4字节，说明不是MySQL的标准格式
            // MySQL的Geometry数据至少包含4字节SRID + WKB数据，长度应该>=4
            // 这种情况可能是异常数据，但为了兼容性，直接返回（可能是损坏的数据或特殊情况）
            return bytes;
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            byte[] wkbData = (byte[]) val;
            // 获取SRID（如果未设置，使用默认值0）
            Integer srid = field.getSrid();
            if (srid == null) {
                srid = 0; // MySQL默认SRID为0
            }
            
            // 转换为MySQL格式：[SRID(4字节)] + [WKB数据]
            ByteBuffer buffer = ByteBuffer.allocate(4 + wkbData.length)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(srid)
                    .put(wkbData);
            return buffer.array();
        }
        return throwUnsupportedException(val, field);
    }
}