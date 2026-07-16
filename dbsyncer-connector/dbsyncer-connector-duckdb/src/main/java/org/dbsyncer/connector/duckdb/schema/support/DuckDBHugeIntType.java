/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.schema.support;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DuckDB HUGEINT / UHUGEINT，按字符串传输避免精度丢失
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 10:00
 */
public final class DuckDBHugeIntType extends StringType {

    private enum TypeEnum {
        HUGEINT("HUGEINT"),
        UHUGEINT("UHUGEINT");

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
    protected String merge(Object val, Field field) {
        if (val == null) {
            return null;
        }
        if (val instanceof BigInteger) {
            return val.toString();
        }
        if (val instanceof Number) {
            return String.valueOf(val);
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
