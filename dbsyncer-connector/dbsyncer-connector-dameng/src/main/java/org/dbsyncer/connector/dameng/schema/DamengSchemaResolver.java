/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.dameng.DamengException;
import org.dbsyncer.connector.dameng.schema.support.DamengBooleanType;
import org.dbsyncer.connector.dameng.schema.support.DamengByteType;
import org.dbsyncer.connector.dameng.schema.support.DamengBytesType;
import org.dbsyncer.connector.dameng.schema.support.DamengDecimalType;
import org.dbsyncer.connector.dameng.schema.support.DamengDoubleType;
import org.dbsyncer.connector.dameng.schema.support.DamengFloatType;
import org.dbsyncer.connector.dameng.schema.support.DamengIntType;
import org.dbsyncer.connector.dameng.schema.support.DamengLongType;
import org.dbsyncer.connector.dameng.schema.support.DamengShortType;
import org.dbsyncer.connector.dameng.schema.support.DamengStringType;
import org.dbsyncer.connector.dameng.schema.support.DamengTimeType;
import org.dbsyncer.connector.dameng.schema.support.DamengTimestampType;
import org.dbsyncer.connector.oracle.schema.OracleSchemaResolver;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.DataType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 达梦标准数据类型解析器
 * <p>在 Oracle 类型映射之上补充达梦特有类型名（如 INT、VARCHAR、DATETIME、IMAGE 等）。</p>
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 02:00
 */
public final class DamengSchemaResolver extends OracleSchemaResolver {

    private static final Map<String, DataType> DAMENG_TYPES = new HashMap<>();

    static {
        Stream.of(
                new DamengBooleanType(),
                new DamengByteType(),
                new DamengBytesType(),
                new DamengDecimalType(),
                new DamengDoubleType(),
                new DamengFloatType(),
                new DamengIntType(),
                new DamengLongType(),
                new DamengShortType(),
                new DamengStringType(),
                new DamengTimeType(),
                new DamengTimestampType()
        ).forEach(DamengSchemaResolver::register);
    }

    private static void register(DataType dataType) {
        dataType.getSupportedTypeName().forEach(typeName -> {
            if (DAMENG_TYPES.containsKey(typeName)) {
                throw new DamengException("Duplicate type name: " + typeName);
            }
            DAMENG_TYPES.put(typeName, dataType);
        });
    }

    /**
     * 规范化类型名：大写、去掉精度括号。
     * <p>例如 {@code TIMESTAMP(6) WITH TIME ZONE} → {@code TIMESTAMP WITH TIME ZONE}</p>
     */
    public static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return null;
        }
        String normalized = typeName.trim().toUpperCase(Locale.ROOT);
        normalized = normalized.replaceAll("\\(\\s*\\d+\\s*\\)", "");
        return normalized.replaceAll("\\s+", " ").trim();
    }

    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        String normalized = normalizeTypeName(field.getTypeName());
        DataType damengType = DAMENG_TYPES.get(normalized);
        if (damengType != null) {
            return damengType;
        }
        // 时间间隔类型按字符串处理
        if (normalized != null && normalized.startsWith("INTERVAL")) {
            return DAMENG_TYPES.get("VARCHAR");
        }
        DataType oracleType = mapping.get(normalized);
        if (oracleType != null) {
            return oracleType;
        }
        return mapping.get(field.getTypeName());
    }

    @Override
    public Object merge(Object val, Field field) {
        return super.merge(val, normalizeField(field));
    }

    @Override
    public Object convert(Object val, Field field) {
        return super.convert(val, normalizeField(field));
    }

    private Field normalizeField(Field field) {
        String normalized = normalizeTypeName(field.getTypeName());
        if (StringUtil.equals(normalized, field.getTypeName())) {
            return field;
        }
        Field copy = new Field(field.getName(), normalized, field.getType(), field.isPk(), field.getColumnSize(), field.getRatio());
        copy.setLabelName(field.getLabelName());
        return copy;
    }
}
