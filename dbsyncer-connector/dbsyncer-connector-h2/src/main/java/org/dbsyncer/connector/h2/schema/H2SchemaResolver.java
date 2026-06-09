/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.h2.schema;

import org.dbsyncer.connector.h2.H2Exception;
import org.dbsyncer.connector.h2.schema.support.H2BooleanType;
import org.dbsyncer.connector.h2.schema.support.H2BytesType;
import org.dbsyncer.connector.h2.schema.support.H2DoubleType;
import org.dbsyncer.connector.h2.schema.support.H2IntType;
import org.dbsyncer.connector.h2.schema.support.H2LongType;
import org.dbsyncer.connector.h2.schema.support.H2StringType;
import org.dbsyncer.connector.h2.schema.support.H2TimestampType;
import org.dbsyncer.sdk.schema.AbstractDatabaseSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * H2 类型映射
 */
public final class H2SchemaResolver extends AbstractDatabaseSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(new H2StringType(), new H2IntType(), new H2LongType(), new H2DoubleType(), new H2BytesType(), new H2TimestampType(), new H2BooleanType()).forEach(t->t.getSupportedTypeName().forEach(typeName-> {
            if (mapping.containsKey(typeName)) {
                throw new H2Exception("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}
