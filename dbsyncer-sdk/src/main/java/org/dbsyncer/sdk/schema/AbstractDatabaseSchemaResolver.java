/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

import java.util.Locale;
import java.util.Map;

/**
 * 关系性数据标准解析器
 * <p>JDBC / 映射配置中的类型名常带长度或精度（如 {@code VARCHAR(255)}、{@code DECIMAL(10,2)}），
 * 查找映射前会去掉数字括号，避免 merge/convert 因类型名无法精确匹配而失败。</p>
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 23:42
 */
public abstract class AbstractDatabaseSchemaResolver extends AbstractSchemaResolver {

    /**
     * 规范化类型名：去空白、转大写、去掉数字精度括号。
     * <p>例如 {@code VARCHAR(255)} → {@code VARCHAR}，{@code DECIMAL(10,2)} → {@code DECIMAL}，
     * {@code TIMESTAMP(6) WITH TIME ZONE} → {@code TIMESTAMP WITH TIME ZONE}。</p>
     *
     * @param typeName 原始类型名
     * @return 规范化后的类型名；入参为 null 时返回 null
     */
    protected String normalizeDatabaseTypeName(String typeName) {
        if (typeName == null) {
            return null;
        }
        String normalized = typeName.trim().toUpperCase(Locale.ROOT);
        normalized = normalized.replaceAll("\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\)", "");
        return normalized.replaceAll("\\s+", " ").trim();
    }

    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        if (field == null) {
            return null;
        }
        DataType dataType = mapping.get(field.getTypeName());
        if (dataType != null) {
            return dataType;
        }
        String normalized = normalizeDatabaseTypeName(field.getTypeName());
        if (normalized == null) {
            return null;
        }
        dataType = mapping.get(normalized);
        if (dataType != null) {
            return dataType;
        }
        return mapping.get(normalized.toLowerCase(Locale.ROOT));
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
        if (field == null) {
            return null;
        }
        String normalized = normalizeDatabaseTypeName(field.getTypeName());
        if (StringUtil.equals(normalized, field.getTypeName())) {
            return field;
        }
        Field copy = new Field(field.getName(), normalized, field.getType(), field.isPk(), field.getColumnSize(), field.getRatio());
        copy.setLabelName(field.getLabelName());
        return copy;
    }

}
