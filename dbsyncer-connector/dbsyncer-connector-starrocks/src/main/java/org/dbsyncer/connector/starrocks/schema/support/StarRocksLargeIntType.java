/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks.schema.support;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public final class StarRocksLargeIntType extends StringType {

    private enum TypeEnum {
        LARGEINT;

        public String getValue() {
            return name();
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val == null) {
            return null;
        }
        if (val instanceof BigInteger) {
            return val.toString();
        }
        if (val instanceof Number) {
            return String.valueOf(((Number) val).longValue());
        }
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof BigInteger) {
            return val.toString();
        }
        if (val instanceof Number) {
            return String.valueOf(val);
        }
        return JsonUtil.objToJsonSafe(val);
    }
}
