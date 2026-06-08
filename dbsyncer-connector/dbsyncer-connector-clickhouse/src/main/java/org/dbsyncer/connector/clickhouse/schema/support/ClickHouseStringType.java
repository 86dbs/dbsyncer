/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseStringType extends StringType {

    private enum TypeEnum {
        STRING("string"),
        FIXEDSTRING("fixedstring"),
        UUID("uuid"),
        ENUM8("enum8"),
        ENUM16("enum16"),
        IPV4("ipv4"),
        IPV6("ipv6");

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
        if (val instanceof String) {
            return (String) val;
        }
        return String.valueOf(val);
    }
}
