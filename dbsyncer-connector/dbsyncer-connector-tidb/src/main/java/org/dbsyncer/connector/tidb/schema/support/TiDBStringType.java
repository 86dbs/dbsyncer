/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.tidb.schema.support;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 00:10
 */
public final class TiDBStringType extends StringType {

    private enum TypeEnum {
        VECTOR;

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
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        return JsonUtil.objToJsonSafe(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        return JsonUtil.objToJsonSafe(val);
    }
}
