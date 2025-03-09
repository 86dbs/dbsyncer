/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-25 00:03
 */
public final class OracleBytesType extends BytesType {

    private enum TypeEnum {
        BLOB("BLOB"),
        RAW("RAW"),
        LONG_RAW("LONG RAW"),
        BFILE("BFILE");

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
    protected byte[] merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            String s = (String) val;
            if (s.startsWith("HEXTORAW(")) {
                return StringUtil.hexStringToByteArray(s.replace("HEXTORAW('", "").replace("')", ""));
            }
            if ("EMPTY_BLOB()".equals(s)) {
                return null;
            }
        }
        return super.convert(val, field);
    }
}