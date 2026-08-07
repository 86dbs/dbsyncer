/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbaseoracle.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oceanbaseoracle.OceanBaseOracleException;
import org.dbsyncer.connector.oceanbaseoracle.schema.support.OceanBaseOracleBytesType;
import org.dbsyncer.connector.oceanbaseoracle.schema.support.OceanBaseOracleDateType;
import org.dbsyncer.connector.oceanbaseoracle.schema.support.OceanBaseOracleDecimalType;
import org.dbsyncer.connector.oceanbaseoracle.schema.support.OceanBaseOracleDoubleType;
import org.dbsyncer.connector.oceanbaseoracle.schema.support.OceanBaseOracleFloatType;
import org.dbsyncer.connector.oceanbaseoracle.schema.support.OceanBaseOracleStringType;
import org.dbsyncer.connector.oceanbaseoracle.schema.support.OceanBaseOracleTimestampType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDatabaseSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * OceanBase Oracle 模式标准数据类型解析器（标准 JDBC，不依赖 oracle.sql.*）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 14:10
 */
public final class OceanBaseOracleSchemaResolver extends AbstractDatabaseSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new OceanBaseOracleBytesType(),
                new OceanBaseOracleDateType(),
                new OceanBaseOracleDecimalType(),
                new OceanBaseOracleDoubleType(),
                new OceanBaseOracleFloatType(),
                new OceanBaseOracleStringType(),
                new OceanBaseOracleTimestampType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new OceanBaseOracleException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
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
        normalized = normalized.replaceAll("\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\)", "");
        return normalized.replaceAll("\\s+", " ").trim();
    }

    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        String normalized = normalizeTypeName(field.getTypeName());
        DataType dataType = mapping.get(normalized);
        if (dataType != null) {
            return dataType;
        }
        if (normalized != null && normalized.startsWith("INTERVAL")) {
            return mapping.get("VARCHAR2");
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
