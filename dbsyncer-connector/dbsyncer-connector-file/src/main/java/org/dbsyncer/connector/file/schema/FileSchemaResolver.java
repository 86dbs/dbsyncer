/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.file.schema;

import org.dbsyncer.connector.file.FileException;
import org.dbsyncer.connector.file.schema.support.FileBooleanType;
import org.dbsyncer.connector.file.schema.support.FileByteType;
import org.dbsyncer.connector.file.schema.support.FileBytesType;
import org.dbsyncer.connector.file.schema.support.FileDateType;
import org.dbsyncer.connector.file.schema.support.FileDecimalType;
import org.dbsyncer.connector.file.schema.support.FileDoubleType;
import org.dbsyncer.connector.file.schema.support.FileFloatType;
import org.dbsyncer.connector.file.schema.support.FileIntType;
import org.dbsyncer.connector.file.schema.support.FileLongType;
import org.dbsyncer.connector.file.schema.support.FileShortType;
import org.dbsyncer.connector.file.schema.support.FileStringType;
import org.dbsyncer.connector.file.schema.support.FileTimeType;
import org.dbsyncer.connector.file.schema.support.FileTimestampType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-13 00:08
 */
public final class FileSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(new FileStringType(), new FileIntType(), new FileShortType(), new FileLongType(), new FileDecimalType(), new FileFloatType(), new FileDoubleType(), new FileDateType(), new FileTimestampType(), new FileTimeType(), new FileBooleanType(), new FileBytesType(), new FileByteType())
                .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
                    if (mapping.containsKey(typeName)) {
                        throw new FileException("Duplicate type name: " + typeName);
                    }
                    mapping.put(typeName, t);
                }));
    }
}