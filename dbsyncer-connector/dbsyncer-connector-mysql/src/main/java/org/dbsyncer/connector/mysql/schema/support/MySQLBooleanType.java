/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLBooleanType extends BooleanType {

    private enum TypeEnum {
        BIT
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Boolean merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Boolean getDefaultMergedVal() {
        return false;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if(val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return (short) (b ? 1 : 0);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}