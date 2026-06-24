/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse.schema;

import org.dbsyncer.connector.clickhouse.ClickHouseException;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseBooleanType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseBytesType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseDateType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseDecimalType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseDoubleType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseFloatType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseIntType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseLongType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseStringType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseTimeType;
import org.dbsyncer.connector.clickhouse.schema.support.ClickHouseTimestampType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDatabaseSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * ClickHouse 标准数据类型解析器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseSchemaResolver extends AbstractDatabaseSchemaResolver {

    public static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return null;
        }
        String normalized = typeName.trim().toLowerCase(Locale.ROOT);
        if (normalized.startsWith("nullable(") && normalized.endsWith(")")) {
            normalized = normalized.substring(9, normalized.length() - 1).trim();
        }
        if (normalized.startsWith("lowcardinality(") && normalized.endsWith(")")) {
            normalized = normalized.substring(15, normalized.length() - 1).trim();
        }
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
                new ClickHouseStringType(),
                new ClickHouseIntType(),
                new ClickHouseLongType(),
                new ClickHouseDecimalType(),
                new ClickHouseFloatType(),
                new ClickHouseDoubleType(),
                new ClickHouseDateType(),
                new ClickHouseTimestampType(),
                new ClickHouseTimeType(),
                new ClickHouseBooleanType(),
                new ClickHouseBytesType())
                .forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
                    if (mapping.containsKey(typeName)) {
                        throw new ClickHouseException("Duplicate type name: " + typeName);
                    }
                    mapping.put(typeName, t);
                }));
    }
}
