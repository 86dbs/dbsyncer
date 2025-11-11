package org.dbsyncer.connector.oracle.schema.support;

import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.geometry.JGeometry;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.GeometryType;
import oracle.sql.STRUCT;

import java.sql.Struct;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Oracle GEOMETRY类型支持
 * Oracle的Geometry类型使用SDO_GEOMETRY（STRUCT类型）存储
 * 注意：Oracle的Geometry类型需要通过JGeometry进行转换
 */
public final class OracleGeometryType extends GeometryType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("SDO_GEOMETRY", "GEOMETRY"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        // GeometryType 重写了 mergeValue 方法，确保即使 val 是 byte[] 类型，也会调用此 merge 方法
        // Oracle从JDBC读取SDO_GEOMETRY数据时，通常返回Struct对象
        // 但如果通过特殊方式获取，可能会返回byte[]格式的pickle数据
        if (val instanceof byte[]) {
            // Oracle从JDBC读取SDO_GEOMETRY数据时，通常返回Struct对象，不会直接返回byte[]
            // 如果收到byte[]，可能是通过特殊方式获取的Oracle内部二进制格式
            // Oracle的SDO_GEOMETRY可以通过JGeometry.store()序列化为byte[]（pickle格式）
            // 需要将pickle格式转换为标准WKB格式
            try {
                // 使用JGeometry.load(byte[])解析pickle格式
                JGeometry jGeometry = JGeometry.load((byte[]) val);
                
                // 提取SRID
                int srid = jGeometry.getSRID();
                field.setSrid(srid);
                
                // 将JGeometry转换为WKT，再转换为标准WKB
                String wkt = jGeometry.toString();
                
                // 从WKT转换为标准WKB
                WKTReader wktReader = new WKTReader();
                com.vividsolutions.jts.geom.Geometry jtsGeometry = wktReader.read(wkt);
                jtsGeometry.setSRID(srid);
                
                WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
                return wkbWriter.write(jtsGeometry);
            } catch (Exception e) {
                throw new OracleException("Failed to convert Oracle geometry from byte array to WKB: " + e.getMessage(), e);
            }
        }
        if (val instanceof Struct) {
            // Oracle的SDO_GEOMETRY是STRUCT类型，需要通过JGeometry转换为WKB
            try {
                // 从STRUCT加载JGeometry（需要转换为oracle.sql.STRUCT）
                Struct struct = (Struct) val;
                if (struct instanceof STRUCT) {
                    JGeometry jGeometry = JGeometry.load((STRUCT) struct);
                    
                    // 提取SRID
                    int srid = jGeometry.getSRID();
                    field.setSrid(srid);
                    
                    // 将JGeometry转换为WKT，再转换为标准WKB
                    String wkt = jGeometry.toString();
                    
                    // 从WKT转换为标准WKB
                    WKTReader wktReader = new WKTReader();
                    com.vividsolutions.jts.geom.Geometry jtsGeometry = wktReader.read(wkt);
                    jtsGeometry.setSRID(srid);
                    
                    WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
                    return wkbWriter.write(jtsGeometry);
                } else {
                    throw new OracleException("Oracle SDO_GEOMETRY must be oracle.sql.STRUCT type");
                }
            } catch (Exception e) {
                throw new OracleException("Failed to convert Oracle SDO_GEOMETRY to WKB: " + e.getMessage(), e);
            }
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            // 标准WKB格式转换为Oracle的SDO_GEOMETRY格式
            // 注意：实际转换为STRUCT需要在ValueMapper中完成，这里只返回byte[]
            // ValueMapper会使用JGeometry.store()方法将WKB转换为STRUCT
            // 这里我们确保byte[]是标准WKB格式，SRID信息通过Field传递
            return val;
        }
        return throwUnsupportedException(val, field);
    }
}

