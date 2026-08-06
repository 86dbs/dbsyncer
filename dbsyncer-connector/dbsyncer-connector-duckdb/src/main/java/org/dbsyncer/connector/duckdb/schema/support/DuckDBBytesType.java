/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DuckDB 二进制类型
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 10:00
 */
public final class DuckDBBytesType extends BytesType {

    private enum TypeEnum {
        BLOB("BLOB"),
        BYTEA("BYTEA"),
        VARBINARY("VARBINARY"),
        BINARY("BINARY"),
        TINYBLOB("TINYBLOB"),
        MEDIUMBLOB("MEDIUMBLOB"),
        LONGBLOB("LONGBLOB");

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
}
