/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.LongType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQLite INTEGER 存储类 - 整数亲和性
 * 支持所有整数相关的类型声明
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>INTEGER</b> - 原生存储类。有符号整数，存储为 1、2、3、4、6 或 8 字节，取决于值的大小</li>
 *   <li><b>INT</b> - 非原生类型，映射到 INTEGER 亲和性。INTEGER 的别名</li>
 *   <li><b>TINYINT</b> - 非原生类型，映射到 INTEGER 亲和性。小整数（通常 -128 到 127）</li>
 *   <li><b>SMALLINT</b> - 非原生类型，映射到 INTEGER 亲和性。小整数（通常 -32768 到 32767）</li>
 *   <li><b>MEDIUMINT</b> - 非原生类型，映射到 INTEGER 亲和性。中等整数（通常 -8388608 到 8388607）</li>
 *   <li><b>BIGINT</b> - 非原生类型，映射到 INTEGER 亲和性。大整数（通常 -9223372036854775808 到 9223372036854775807）</li>
 *   <li><b>INT2</b> - 非原生类型，映射到 INTEGER 亲和性。2字节整数（功能与 SMALLINT 相同）</li>
 *   <li><b>INT8</b> - 非原生类型，映射到 INTEGER 亲和性。8字节整数（功能与 BIGINT 相同）</li>
 *   <li><b>BOOLEAN</b> - 非原生类型，映射到 INTEGER 或 NUMERIC 亲和性。布尔值，存储为 0 或 1</li>
 * </ul>
 * 所有类型最终存储为 INTEGER 存储类，SQLite 会根据值的大小自动选择存储字节数。
 * </p>
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteIntegerType extends LongType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("INTEGER", "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "INT2", "INT8", "BOOLEAN"));
    }

    @Override
    protected Long merge(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        if (val instanceof String) {
            try {
                return Long.parseLong((String) val);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
        return 0L;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Long) {
            return val;
        }
        if (val instanceof Integer) {
            return ((Integer) val).longValue();
        }
        if (val instanceof Boolean) {
            return ((Boolean) val) ? 1L : 0L;
        }
        return super.convert(val, field);
    }
}
