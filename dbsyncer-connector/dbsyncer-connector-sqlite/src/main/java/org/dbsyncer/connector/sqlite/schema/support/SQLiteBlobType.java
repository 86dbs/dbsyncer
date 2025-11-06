/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQLite BLOB 存储类 - 二进制亲和性
 * 支持所有二进制数据相关的类型声明
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>BLOB</b> - 原生存储类。二进制大对象，存储为输入的精确副本，不进行任何转换。
 *       用于存储任意二进制数据，如图片、文件、加密数据等。</li>
 * </ul>
 * BLOB 存储类不限制数据长度（受限于数据库文件大小），数据以原始字节形式存储。
 * </p>
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class SQLiteBlobType extends BytesType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BLOB"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val instanceof String) {
            return ((String) val).getBytes();
        }
        return new byte[0];
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            return val;
        }
        if (val instanceof String) {
            return ((String) val).getBytes();
        }
        return super.convert(val, field);
    }
}
