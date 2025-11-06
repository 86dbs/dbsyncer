package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TimeType;

import java.sql.Time;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite TIME类型支持
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>TIME</b> - 非原生类型，映射到 NUMERIC 或 TEXT 亲和性（通常为 TEXT）。
 *       时间类型，SQLite 没有专门的时间数据类型，通常存储为 TEXT（ISO8601 格式：HH:MM:SS）。
 *       也可以存储为 INTEGER（Unix 时间戳）或 REAL（儒略日数）。</li>
 * </ul>
 * SQLite 提供日期时间函数（time(), datetime(), strftime() 等）来处理这些值。
 * </p>
 */
public final class SQLiteTimeType extends TimeType {

    private enum TypeEnum {
        TIME         // 时间类型
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected Time merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }
}

