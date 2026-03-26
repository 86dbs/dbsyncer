/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-25 00:03
 */
public final class OracleTimestampType extends TimestampType {

    private enum TypeEnum {

        TIMESTAMP("TIMESTAMP(6)"), TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP(6) WITH TIME ZONE"), TIMESTAMP_WITH_LOCAL_TIME_ZONE("TIMESTAMP(6) WITH LOCAL TIME ZONE");

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
    protected Timestamp merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

}