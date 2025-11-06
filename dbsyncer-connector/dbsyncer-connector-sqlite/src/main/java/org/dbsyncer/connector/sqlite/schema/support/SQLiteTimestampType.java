package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite TIMESTAMP类型支持
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>TIMESTAMP</b> - 非原生类型，映射到 NUMERIC 或 TEXT 亲和性（通常为 TEXT）。
 *       时间戳类型，SQLite 没有专门的时间戳数据类型，通常存储为 TEXT（ISO8601 格式：YYYY-MM-DD HH:MM:SS）。
 *       也可以存储为 INTEGER（Unix 时间戳，自 1970-01-01 00:00:00 UTC 以来的秒数）或 REAL（儒略日数，自公元前 4714 年 11 月 24 日格林尼治中午以来的天数）。
 *       与 DATETIME 功能相同。</li>
 * </ul>
 * SQLite 提供日期时间函数（datetime(), strftime(), julianday() 等）来处理这些值。
 * </p>
 */
public final class SQLiteTimestampType extends TimestampType {

    private enum TypeEnum {
        TIMESTAMP    // 时间戳类型（功能与 DATETIME 相同）
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Timestamp merge(Object val, Field field) {
        if (val instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) val;
            return new Timestamp(date.getTime());
        }
        return throwUnsupportedException(val, field);
    }
}

