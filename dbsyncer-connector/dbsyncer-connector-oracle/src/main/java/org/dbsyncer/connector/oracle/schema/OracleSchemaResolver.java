/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema;

import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.schema.support.*;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class OracleSchemaResolver extends AbstractSchemaResolver {
    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
                new OracleBytesType(),
                new OracleDateType(),
                new OracleDecimalType(),
                new OracleDoubleType(),
                new OracleFloatType(),
                new OracleIntType(),
                new OracleLongType(),
                new OracleStringType(),
                new OracleTimestampType()
        ).forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new OracleException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}
