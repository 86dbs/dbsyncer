/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema;

import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBooleanType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBytesType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDateType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDecimalType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDoubleType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLFloatType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLIntType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLLongType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLStringType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLTimestampType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * PostgreSQL标准数据类型解析器
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:01
 */
public final class PostgreSQLSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(new PostgreSQLStringType(), new PostgreSQLIntType(), new PostgreSQLLongType(), new PostgreSQLDecimalType(), new PostgreSQLFloatType(), new PostgreSQLDoubleType(), new PostgreSQLDateType(), new PostgreSQLTimestampType(), new PostgreSQLBooleanType(), new PostgreSQLBytesType())
                .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
                    if (mapping.containsKey(typeName)) {
                        throw new PostgreSQLException("Duplicate type name: " + typeName);
                    }
                    mapping.put(typeName, t);
                }));
    }
}