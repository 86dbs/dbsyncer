/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ShortType;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLShortType extends ShortType {

    private enum TypeEnum {
        TINYINT_UNSIGNED("TINYINT UNSIGNED"),
        SMALLINT("SMALLINT");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected Short merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
//        if (val instanceof BitSet) {
//            BitSet bitSet = (BitSet) val;
//            byte[] bytes = bitSet.toByteArray();
//            if (bytes.length > 0) {
//                return (short) bytes[0];
//            }
//            return 0;
//        }
//        if (val instanceof Boolean) {
//            Boolean b = (Boolean) val;
//            return (short) (b ? 1 : 0);
//        }
//        if (val instanceof byte[]) {
//            byte[] bytes = (byte[]) val;
//            if (bytes.length > 1) {
//                return (short) bytes[1];
//            }
//        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        return throwUnsupportedException(val, field);
    }

}