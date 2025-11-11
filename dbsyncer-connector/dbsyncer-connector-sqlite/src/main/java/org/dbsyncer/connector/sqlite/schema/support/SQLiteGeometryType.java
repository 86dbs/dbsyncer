package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.GeometryType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQLite GEOMETRY类型支持
 * SQLite不原生支持Geometry类型，但可以通过BLOB存储WKB格式的几何数据
 * 或者通过SpatiaLite扩展支持Geometry类型
 * 注意：SQLite的Geometry数据以标准WKB格式存储（不带SRID前缀）
 * SRID信息需要单独存储或通过SpatiaLite扩展管理
 */
public final class SQLiteGeometryType extends GeometryType {

    @Override
    public Set<String> getSupportedTypeName() {
        // SQLite通过SpatiaLite扩展支持Geometry类型
        return new HashSet<>(Arrays.asList("GEOMETRY", "POINT", "LINESTRING", "POLYGON", 
                "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            // SQLite的Geometry数据以BLOB格式存储标准WKB格式（不带SRID）
            // SQLite从JDBC读取BLOB数据时，直接返回byte[]
            // SQLite本身不存储SRID在数据中，如果使用SpatiaLite扩展，SRID存储在单独的列中
            // 因此从SQLite读取的byte[]已经是标准WKB格式，直接返回即可
            return (byte[]) val;
        }
        // SQLite不支持Geometry类型，如果没有SpatiaLite扩展，使用BLOB存储
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            // 直接返回标准WKB格式的byte[]
            // SRID信息通过Field传递，如果需要，可以在应用层处理
            return val;
        }
        return throwUnsupportedException(val, field);
    }
}

