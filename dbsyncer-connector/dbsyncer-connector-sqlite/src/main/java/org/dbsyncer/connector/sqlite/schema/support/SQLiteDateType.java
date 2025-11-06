package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DateType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite DATE类型支持
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>DATE</b> - 非原生类型，映射到 NUMERIC 或 TEXT 亲和性（通常为 TEXT）。
 *       日期类型，SQLite 没有专门的日期数据类型，通常存储为 TEXT（ISO8601 格式：YYYY-MM-DD）。
 *       也可以存储为 INTEGER（Unix 时间戳）或 REAL（儒略日数）。</li>
 *   <li><b>DATETIME</b> - 非原生类型，映射到 NUMERIC 或 TEXT 亲和性（通常为 TEXT）。
 *       日期时间类型，通常存储为 TEXT（ISO8601 格式：YYYY-MM-DD HH:MM:SS）。
 *       也可以存储为 INTEGER（Unix 时间戳）或 REAL（儒略日数）。
 *       与 TIMESTAMP 功能相同。</li>
 * </ul>
 * SQLite 提供日期时间函数（date(), datetime(), julianday() 等）来处理这些值。
 * </p>
 */
public final class SQLiteDateType extends DateType {

    private enum TypeEnum {
        DATE,        // 日期类型
        DATETIME     // 日期时间类型（功能与 TIMESTAMP 相同）
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Date merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }
}

