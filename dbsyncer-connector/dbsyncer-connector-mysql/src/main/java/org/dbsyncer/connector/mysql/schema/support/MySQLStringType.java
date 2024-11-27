/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:24
 */
public final class MySQLStringType extends StringType {

    private enum TypeEnum {
        CHAR, // 固定长度，最多255个字符
        VARCHAR, // 固定长度，最多65535个字符，64K
        TINYTEXT, // 可变长度，最多255字符
        TEXT, // 可变长度，最多65535个字符，64K
        MEDIUMTEXT, // 可变长度，最多2的24次方-1个字符，16M
        LONGTEXT, // 可变长度，最多2的32次方-1个字符，4GB
        ENUM, // 2字节，最大可达65535个不同的枚举值
        JSON, GEOMETRY, POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        switch (TypeEnum.valueOf(field.getTypeName())) {
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
                return String.valueOf(val);

            default:
                return throwUnsupportedException(val, field);
        }
    }

    @Override
    protected String getDefaultMergedVal() {
        return "";
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

}