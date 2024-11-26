/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.DataType;
import org.dbsyncer.sdk.schema.support.StringType;

import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:24
 */
public class MySQLStringType extends StringType {
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
    public void postProcessBeforeInitialization(Map<String, DataType> mapping) {
        mapping.put("CHAR", this); // 固定长度，最多255个字符
        mapping.put("VARCHAR", this); // 固定长度，最多65535个字符，64K
        mapping.put("TINYTEXT", this); // 可变长度，最多255字符
        mapping.put("TEXT", this); // 可变长度，最多65535个字符，64K
        mapping.put("MEDIUMTEXT", this); // 可变长度，最多2的24次方-1个字符，16M
        mapping.put("LONGTEXT", this); // 可变长度，最多2的32次方-1个字符，4GB
        mapping.put("ENUM", this); // 2字节，最大可达65535个不同的枚举值
        mapping.put("JSON", this);
        mapping.put("GEOMETRY", this);
        mapping.put("POINT", this);
        mapping.put("LINESTRING", this);
        mapping.put("POLYGON", this);
        mapping.put("MULTIPOINT", this);
        mapping.put("MULTILINESTRING", this);
        mapping.put("MULTIPOLYGON", this);
        mapping.put("GEOMETRYCOLLECTION", this);
    }
}