/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.doris.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.doris.DorisException;
import org.dbsyncer.connector.doris.schema.support.DorisBooleanType;
import org.dbsyncer.connector.doris.schema.support.DorisLargeIntType;
import org.dbsyncer.connector.doris.schema.support.DorisStringType;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.DataType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Doris 标准数据类型解析器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 23:50
 */
public final class DorisSchemaResolver extends MySQLSchemaResolver {

    private static final Map<String, DataType> DORIS_TYPES = new HashMap<>();

    static {
        register(new DorisStringType());
        register(new DorisLargeIntType());
        register(new DorisBooleanType());
    }

    private static void register(DataType dataType) {
        dataType.getSupportedTypeName().forEach(typeName -> {
            if (DORIS_TYPES.containsKey(typeName)) {
                throw new DorisException("Duplicate type name: " + typeName);
            }
            DORIS_TYPES.put(typeName, dataType);
        });
    }

    public static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return null;
        }
        String normalized = typeName.trim().toUpperCase(Locale.ROOT);
        if (normalized.startsWith("ARRAY")) {
            return "ARRAY";
        }
        if (normalized.startsWith("MAP")) {
            return "MAP";
        }
        if (normalized.startsWith("STRUCT")) {
            return "STRUCT";
        }
        int parenIndex = normalized.indexOf('(');
        if (parenIndex > 0) {
            normalized = normalized.substring(0, parenIndex).trim();
        }
        return normalized;
    }

    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        String normalized = normalizeTypeName(field.getTypeName());
        DataType dorisType = DORIS_TYPES.get(normalized);
        if (dorisType != null) {
            return dorisType;
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
