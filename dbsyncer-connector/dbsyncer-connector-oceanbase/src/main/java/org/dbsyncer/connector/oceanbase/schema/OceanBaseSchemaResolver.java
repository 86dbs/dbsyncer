/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase.schema;

import org.dbsyncer.connector.oceanbase.OceanBaseException;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseByteType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseBytesType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseDateType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseDecimalType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseDoubleType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseFloatType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseIntType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseLongType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseShortType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseStringType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseTimeType;
import org.dbsyncer.connector.oceanbase.schema.support.OceanBaseTimestampType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDatabaseSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * OceanBase标准数据类型解析器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-04 00:20
 */
public final class OceanBaseSchemaResolver extends AbstractDatabaseSchemaResolver {

    /**
     * 规范化类型名：转大写并去除长度等括号参数（如 varchar(64) -> VARCHAR）。
     */
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
        DataType dataType = mapping.get(field.getTypeName());
        if (dataType == null) {
            dataType = mapping.get(normalizeTypeName(field.getTypeName()));
        }
        return dataType;
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
            new OceanBaseBytesType(),
            new OceanBaseByteType(),
            new OceanBaseDateType(),
            new OceanBaseDecimalType(),
            new OceanBaseDoubleType(),
            new OceanBaseFloatType(),
            new OceanBaseIntType(),
            new OceanBaseLongType(),
            new OceanBaseShortType(),
            new OceanBaseStringType(),
            new OceanBaseTimestampType(),
            new OceanBaseTimeType())
        .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
            String key = normalizeTypeName(typeName);
            if (mapping.containsKey(key)) {
                throw new OceanBaseException("Duplicate type name: " + key);
            }
            mapping.put(key, t);
        }));
    }

}