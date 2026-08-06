/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.tidb.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.connector.tidb.TiDBException;
import org.dbsyncer.connector.tidb.schema.support.TiDBStringType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.DataType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * TiDB 标准数据类型解析器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 00:10
 */
public final class TiDBSchemaResolver extends MySQLSchemaResolver {

    private static final Map<String, DataType> TIDB_TYPES = new HashMap<>();

    static {
        register(new TiDBStringType());
    }

    private static void register(DataType dataType) {
        dataType.getSupportedTypeName().forEach(typeName -> {
            if (TIDB_TYPES.containsKey(typeName)) {
                throw new TiDBException("Duplicate type name: " + typeName);
            }
            TIDB_TYPES.put(typeName, dataType);
        });
    }

    public static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return null;
        }
        String normalized = typeName.trim().toUpperCase(Locale.ROOT);
        int parenIndex = normalized.indexOf('(');
        if (parenIndex > 0) {
            normalized = normalized.substring(0, parenIndex).trim();
        }
        return normalized;
    }

    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        String normalized = normalizeTypeName(field.getTypeName());
        DataType tidbType = TIDB_TYPES.get(normalized);
        if (tidbType != null) {
            return tidbType;
        }
        DataType mysqlType = mapping.get(normalized);
        if (mysqlType != null) {
            return mysqlType;
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
