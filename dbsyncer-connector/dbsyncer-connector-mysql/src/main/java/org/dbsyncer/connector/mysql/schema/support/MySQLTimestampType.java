/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLTimestampType extends TimestampType {

    enum TypeEnum {
        DATETIME,
        TIMESTAMP;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(type -> type.name()).collect(Collectors.toSet());
    }

    @Override
    protected Timestamp merge(Object val, Field field) {
        return null;
    }

    @Override
    protected Timestamp getDefaultMergedVal() {
        return null;
    }

    @Override
    protected Object convert(Object val, Field field) {
        return null;
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}