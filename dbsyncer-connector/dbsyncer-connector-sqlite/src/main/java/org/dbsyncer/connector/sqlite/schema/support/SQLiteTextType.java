package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeTextType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite TEXT 存储类 - 文本亲和性
 * 支持所有大文本相关的类型声明
 * <p>
 * SQLite 默认支持 UTF-8（类似 MySQL、PostgreSQL），
 * 因此标准化为 UNICODE_TEXT 以确保数据安全性和跨数据库兼容性。
 * </p>
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>TEXT</b> - 原生存储类。文本字符串，存储为 UTF-8、UTF-16BE 或 UTF-16LE 编码，支持任意长度的文本</li>
 *   <li><b>CLOB</b> - 非原生类型，映射到 TEXT 亲和性。字符大对象，用于存储大量文本数据（功能与 TEXT 相同）</li>
 * </ul>
 * 所有类型最终存储为 TEXT 存储类，SQLite 不限制文本长度（受限于数据库文件大小）。
 * </p>
 */
public final class SQLiteTextType extends UnicodeTextType {

    private enum TypeEnum {
        // TEXT 亲和性类型 - 大文本类型
        TEXT,        // 文本类型（原生存储类）
        CLOB         // 字符大对象
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        return String.valueOf(val);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }
}
