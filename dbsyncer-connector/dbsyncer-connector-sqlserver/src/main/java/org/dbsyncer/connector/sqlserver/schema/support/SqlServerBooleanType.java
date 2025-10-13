/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BooleanType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-04-05
 */
public final class SqlServerBooleanType extends BooleanType {

    private enum TypeEnum {
        BIT("bit");

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
    protected Boolean merge(Object val, Field field) {
        // 直接使用父类的方法将值转换为Boolean
        return (Boolean) super.convertValue(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        // 直接使用父类的方法将值转换为Boolean
        return super.convertValue(val, field);
    }
}