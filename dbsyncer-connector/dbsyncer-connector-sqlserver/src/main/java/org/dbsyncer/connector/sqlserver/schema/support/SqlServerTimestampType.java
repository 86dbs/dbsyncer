/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-04-05
 */
public final class SqlServerTimestampType extends TimestampType {

    private enum TypeEnum {
        DATETIME("datetime"),
        DATETIME2("datetime2"),
        SMALLDATETIME("smalldatetime"),
        TIMESTAMP("timestamp");

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
        // 直接使用父类的convert方法将值转换为Timestamp
        return (Timestamp) super.convert(val, field);
    }

    // convert 方法与父类实现一致，无需重写
}