/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLLongType extends LongType {

    private enum TypeEnum {
        BIGINT("BIGINT"),
        INT_UNSIGNED("INT UNSIGNED"),
        INT_UNSIGNED_ZEROFILL("INT UNSIGNED ZEROFILL"),
        INTEGER_UNSIGNED("INTEGER UNSIGNED");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public TypeEnum getTypeEnum(String value) {
            for (TypeEnum type : TypeEnum.values()) {
                if (StringUtil.equals(value, type.value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Can not find type:" + value);
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
    protected Long merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Long getDefaultMergedVal() {
        return 0L;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}