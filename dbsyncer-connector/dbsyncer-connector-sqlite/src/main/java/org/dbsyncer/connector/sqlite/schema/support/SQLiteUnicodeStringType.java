package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeTextType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite Unicode字符串类型支持
 * <p>
 * SQLite的VARCHAR/CHAR类型默认支持UTF-8（类似MySQL、PostgreSQL），
 * 但SQLite的VARCHAR/CHAR实际上没有长度限制（都存储为TEXT存储类），
 * 因此标准化为UNICODE_TEXT以确保跨数据库同步时的数据安全性。
 * </p>
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>VARCHAR</b> - 非原生类型，映射到 TEXT 亲和性。可变长度字符串，SQLite 默认 UTF-8 编码。
 *       重要：SQLite 的 VARCHAR 没有长度限制，与 MySQL/PostgreSQL 的 VARCHAR（有长度限制）不同。
 *       标准化为 UNICODE_TEXT 以避免同步到有长度限制的目标数据库时出现截断问题。</li>
 *   <li><b>CHAR</b> - 非原生类型，映射到 TEXT 亲和性。固定长度字符串，SQLite 默认 UTF-8 编码。
 *       重要：SQLite 的 CHAR 没有长度限制，与 MySQL/PostgreSQL 的 CHAR（有长度限制）不同。
 *       标准化为 UNICODE_TEXT 以避免同步到有长度限制的目标数据库时出现截断问题。</li>
 *   <li><b>NCHAR</b> - 非原生类型，映射到 TEXT 亲和性。Unicode 字符串（功能与 CHAR 相同，SQLite 的 TEXT 默认就是 UTF-8）</li>
 *   <li><b>NVARCHAR</b> - 非原生类型，映射到 TEXT 亲和性。Unicode 字符串（功能与 VARCHAR 相同，SQLite 的 TEXT 默认就是 UTF-8）</li>
 * </ul>
 * 所有类型最终存储为 TEXT 存储类，支持 UTF-8/UTF-16BE/UTF-16LE 编码，不限制文本长度（受限于数据库文件大小）。
 * </p>
 * <p>
 * <b>为什么使用 UNICODE_TEXT 而不是 UNICODE_STRING：</b>
 * <ul>
 *   <li>SQLite 的 VARCHAR/CHAR 没有长度限制，而 UNICODE_STRING 通常映射到有长度限制的类型（如 MySQL VARCHAR(255)）</li>
 *   <li>标准化为 UNICODE_TEXT 可以确保同步到目标数据库时使用无长度限制或大容量类型（如 MySQL TEXT/LONGTEXT）</li>
 *   <li>避免因长度超过限制导致的数据截断或同步失败</li>
 * </ul>
 * </p>
 */
public final class SQLiteUnicodeStringType extends UnicodeTextType {

    private enum TypeEnum {
        // TEXT 亲和性类型 - 字符串类型
        VARCHAR,     // 可变长度字符串
        CHAR,        // 固定长度字符串
        NCHAR,       // Unicode 字符串（功能与 CHAR 相同）
        NVARCHAR     // Unicode 字符串（功能与 VARCHAR 相同）
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
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }
}

