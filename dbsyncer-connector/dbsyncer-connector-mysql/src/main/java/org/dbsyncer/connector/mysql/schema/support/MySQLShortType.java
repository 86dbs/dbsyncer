/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.ShortType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLShortType extends ShortType {

    private enum TypeEnum {
        TINYINT_UNSIGNED("TINYINT UNSIGNED"),
        TINYINT_UNSIGNED_ZEROFILL("TINYINT UNSIGNED ZEROFILL"),
        SMALLINT("SMALLINT");

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
    protected Short merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Short getDefaultMergedVal() {
        return 0;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).shortValue();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}