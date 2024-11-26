/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:24
 */
public class MySQLStringType extends StringType {

    private final Set<String> supported = new HashSet<>();

    public MySQLStringType() {
        supported.add("CHAR"); // 固定长度，最多255个字符
        supported.add("VARCHAR"); // 固定长度，最多65535个字符，64K
        supported.add("TINYTEXT"); // 可变长度，最多255字符
        supported.add("TEXT"); // 可变长度，最多65535个字符，64K
        supported.add("MEDIUMTEXT"); // 可变长度，最多2的24次方-1个字符，16M
        supported.add("LONGTEXT"); // 可变长度，最多2的32次方-1个字符，4GB
        supported.add("ENUM"); // 2字节，最大可达65535个不同的枚举值
        supported.add("JSON");
        supported.add("GEOMETRY");
        supported.add("POINT");
        supported.add("LINESTRING");
        supported.add("POLYGON");
        supported.add("MULTIPOINT");
        supported.add("MULTILINESTRING");
        supported.add("MULTIPOLYGON");
        supported.add("GEOMETRYCOLLECTION");
    }

    @Override
    protected String merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

    @Override
    protected String getDefaultMergedVal() {
        return "";
    }

    @Override
    protected Object convert(Object val, Field field) {
        return null;
    }

    @Override
    protected Object getDefaultConvertedVal() {
        return null;
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return supported;
    }
}