/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.connector.sqlserver.SqlServerException;
import org.dbsyncer.connector.sqlserver.schema.SqlServerGeographyData;
import org.dbsyncer.connector.sqlserver.schema.SqlServerGeometryData;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

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

        CHAR("char"), VARCHAR("varchar"), NVARCHAR("nvarchar"), TEXT("text"), GEOMETRY("geometry"), GEOGRAPHY("geography");

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
            TypeEnum type = TypeEnum.getType(field.getTypeName());
            if (type != null) {
                switch (type) {
                    case GEOMETRY:
                    case GEOGRAPHY:
                        return parseToWKTWithSRID((byte[]) val);
                    default:
                        break;
                }
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
    protected Object getDefaultConvertedVal(Field field) {
        TypeEnum type = TypeEnum.getType(field.getTypeName());
        if (type != null) {
            switch (type) {
                case GEOMETRY:
                    return new SqlServerGeometryData(null);
                case GEOGRAPHY:
                    return new SqlServerGeographyData(null);
                default:
                    break;
            }
        }
        return super.getDefaultConvertedVal(field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            TypeEnum type = TypeEnum.getType(field.getTypeName());
            if (type != null) {
                switch (type) {
                    case GEOMETRY:
                        return new SqlServerGeometryData(val);
                    case GEOGRAPHY:
                        return new SqlServerGeographyData(val);
                    default:
                        break;
                }
            }
        }
        return super.convert(val, field);
    }

    /**
     * 将 SQL Server CDC Geometry 转换为标准 WKB
     * 步骤: 提取坐标 -> 创建标准 WKB -> 使用 WKBReader 解析
     */
    public static String parseToWKTWithSRID(byte[] bytes) {
        try {
            if (bytes == null || bytes.length < 22) {
                return null;
            }

            // 解析 SRID
            int srid = ((bytes[1] & 0xFF) << 8) | (bytes[0] & 0xFF);

            // 解析坐标
            ByteBuffer buffer = ByteBuffer.wrap(bytes, 6, 16);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            double x = buffer.getDouble();
            double y = buffer.getDouble();

            // 使用 EWKB 创建 Geometry
            byte[] ewkb = createEWKBPoint(x, y, srid);
            WKBReader reader = new WKBReader();
            Geometry geometry = reader.read(ewkb);

            return "SRID=" + srid + ";" + geometry.toText();
        } catch (ParseException e) {
            throw new SqlServerException(e);
        }
    }

    /**
     * 创建 EWKB Point
     */
    private static byte[] createEWKBPoint(double x, double y, int srid) {
        // EWKB 结构: 字节顺序(1) + 类型(4) + SRID(4) + 坐标(16)
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + 8 + 8); // 25字节
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // 字节顺序标记
        buffer.put((byte) 1);

        // 几何类型: Point with SRID flag (0x20000000)
        buffer.putInt(1 | 0x20000000);

        // SRID
        buffer.putInt(srid);

        // 坐标
        buffer.putDouble(x);
        buffer.putDouble(y);

        return buffer.array();
    }
}
