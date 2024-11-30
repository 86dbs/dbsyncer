/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLBytesType extends BytesType {

    private enum TypeEnum {
        TINYBLOB,
        BLOB,
        MEDIUMBLOB,
        LONGBLOB,
        BINARY,
        VARBINARY;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Byte[] merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Byte[] getDefaultMergedVal() {
        return new Byte[0];
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Byte[]) {
            return val;
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}