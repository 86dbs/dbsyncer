/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ByteType;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLByteType extends ByteType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BIT", "TINYINT"));
    }

    @Override
    protected Byte merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).byteValue();
        }
        if (val instanceof BitSet) {
            BitSet bitSet = (BitSet) val;
            byte[] bytes = bitSet.toByteArray();
            if (bytes.length > 0) {
                return bytes[0];
            }
            return 0;
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return (byte) (b ? 1 : 0);
        }
        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            if (bytes.length > 1) {
                return bytes[1];
            }
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Byte) {
            return val;
        }
        return super.convert(val, field);
    }

}