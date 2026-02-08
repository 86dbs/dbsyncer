/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
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
public final class PostgreSQLStringType extends StringType {

    private enum TypeEnum {

        // 标准字符串类型
        UUID("uuid"), VARCHAR("varchar"), TEXT("text"), CHAR("char"), BPCHAR("bpchar"), NAME("name"),
        // JSON 类型
        JSON("json"), JSONB("jsonb"),
        // 几何类型
        POINT("point"),
        // PostGIS 扩展类型
        GEOMETRY("geometry"), GEOGRAPHY("geography"),
        // 网络类型
        INET("inet"), CIDR("cidr"), MACADDR("macaddr"), MACADDR8("macaddr8"),
        // 文本搜索类型
        TSQUERY("tsquery"), TSVECTOR("tsvector"),
        // 范围类型
        TSRANGE("tsrange"), TSTZRANGE("tstzrange"), DATERANGE("daterange"), INT4RANGE("int4range"), INT8RANGE("int8range"), NUMRANGE("numrange"),
        // 位串类型
        BIT("bit"), BIT_VARYING("bit varying"), VARBIT("varbit"),
        // 其他类型
        CITEXT("citext"), XML("xml"), PG_LSN("pg_lsn"), TXID_SNAPSHOT("txid_snapshot");

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
        if (val instanceof Boolean) {
            return ((Boolean) val) ? "1" : "0";
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    public Object getDefaultConvertedVal(Field field) {
        try {
            PGobject pgObject = new PGobject();
            pgObject.setType(field.getTypeName());
            pgObject.setValue(null);
            return pgObject;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object convert(Object val, Field field) {
        // 将类型名转大写并替换空格为下划线，以匹配 TypeEnum
        String enumName = field.getTypeName().toUpperCase().replace(" ", "_");
        TypeEnum typeEnum = TypeEnum.valueOf(enumName);
        // BIT 类型需要特殊处理：支持 Integer、Boolean、Number、String 等多种输入
        if (typeEnum == TypeEnum.BIT) {
            return convertToBit(val);
        }

        if (val instanceof String) {
            String strVal = (String) val;
            switch (typeEnum) {
                case UUID:
                    try {
                        return UUIDUtil.fromString(strVal);
                    } catch (IllegalArgumentException e) {
                        // UUID 格式无效时返回 null，避免抛出异常
                        return null;
                    }
                case POINT:
                    return toPoint(strVal);
                case VARCHAR:
                case TEXT:
                case CHAR:
                case BPCHAR:
                case NAME:
                    // 标准字符串类型，直接返回
                    return val;
                // 以下类型需要用 PGobject 包装
                case JSON:
                case JSONB:
                case GEOMETRY:
                case GEOGRAPHY:
                case CITEXT:
                case XML:
                case INET:
                case CIDR:
                case MACADDR:
                case MACADDR8:
                case TSQUERY:
                case TSVECTOR:
                case TSRANGE:
                case TSTZRANGE:
                case DATERANGE:
                case INT4RANGE:
                case INT8RANGE:
                case NUMRANGE:
                case BIT_VARYING:
                case VARBIT:
                case PG_LSN:
                case TXID_SNAPSHOT:
                    try {
                        PGobject pgObject = new PGobject();
                        pgObject.setType(typeEnum.getValue());
                        pgObject.setValue(strVal);
                        return pgObject;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                default:
                    return val;
            }
        }
        return super.convert(val, field);
    }

    /**
     * 将值转换为 PostgreSQL bit 类型（PGobject 包装）。
     * 支持 Integer、Boolean、Number、String 等输入，统一转换为 bit 字符串。
     */
    private Object convertToBit(Object val) {
        String bitStr;
        if (val instanceof Boolean) {
            bitStr = ((Boolean) val) ? "1" : "0";
        } else if (val instanceof Number) {
            // Integer、Short、Long 等：非 0 为 "1"，0 为 "0"
            bitStr = ((Number) val).intValue() != 0 ? "1" : "0";
        } else if (val instanceof String) {
            String s = (String) val;
            // 字符串 "true"/"1" 等转为 "1"，其他转为 "0"
            if ("true".equalsIgnoreCase(s) || "1".equals(s) || "t".equalsIgnoreCase(s)) {
                bitStr = "1";
            } else {
                bitStr = "0";
            }
        } else {
            throw new PostgreSQLException(String.format("Cannot convert %s to bit type, val [%s]", val.getClass(), val));
        }
        try {
            PGobject pgObject = new PGobject();
            pgObject.setType(TypeEnum.BIT.getValue());
            pgObject.setValue(bitStr);
            return pgObject;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
