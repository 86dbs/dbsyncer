/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kingbase.schema;

import com.google.protobuf.ByteString;
import com.kingbase8.geometric.KBpoint;
import com.kingbase8.util.KBobject;
import org.dbsyncer.connector.kingbase.KingbaseException;
import org.dbsyncer.connector.kingbase.schema.support.KingbaseStringType;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBooleanType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLBytesType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDateType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDecimalType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLDoubleType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLFloatType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLIntType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLLongType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLTimeType;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLTimestampType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 18:30
 */
public final class KingbaseSchemaResolver extends PostgreSQLSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(new KingbaseStringType(), new PostgreSQLIntType(), new PostgreSQLLongType(), new PostgreSQLDecimalType(),
                        new PostgreSQLFloatType(), new PostgreSQLDoubleType(), new PostgreSQLDateType(),
                        new PostgreSQLTimestampType(), new PostgreSQLBooleanType(), new PostgreSQLBytesType(), new PostgreSQLTimeType())
                .forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
                    if (mapping.containsKey(typeName)) {
                        throw new KingbaseException("Duplicate type name: " + typeName);
                    }
                    mapping.put(typeName, t);
                }));
    }

    @Override
    public ByteString serialize(Object value, Field field) {
        String type = value.getClass().getName();
        switch (type) {
            case "com.kingbase8.util.KBobject":
                KBobject kbObject = (KBobject) value;
                return ByteString.copyFromUtf8(Objects.requireNonNull(kbObject.getValue()));
            case "com.kingbase8.geometric.KBpoint":
                KBpoint kbPoint = (KBpoint) value;
                return ByteString.copyFromUtf8(Objects.requireNonNull(kbPoint.getValue()));
            default:
                break;
        }
        return super.serialize(value, field);
    }
}
