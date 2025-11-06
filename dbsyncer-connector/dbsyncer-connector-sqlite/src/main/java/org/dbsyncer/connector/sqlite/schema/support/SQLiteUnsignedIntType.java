package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedIntType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite 无符号整型支持
 * <p>
 * SQLite 不区分有符号/无符号，所有整数类型都映射到 INTEGER 存储类。
 * 虽然 SQLite 不支持真正的 UNSIGNED 语法，但在从其他数据库同步表结构时，
 * DDL 可能保留原始类型定义（如 "INT UNSIGNED"），因此我们识别这些类型名
 * 以尊重原始 DDL 定义。实际存储仍为 INTEGER，但类型名会保留。
 * </p>
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>INT UNSIGNED / INTEGER UNSIGNED / MEDIUMINT UNSIGNED</b> - 识别这些类型名以尊重从其他数据库同步时的原始 DDL 定义。
 *       虽然 SQLite 实际存储为 INTEGER，但类型名会保留原始定义。</li>
 * </ul>
 * </p>
 */
public final class SQLiteUnsignedIntType extends UnsignedIntType {

    private enum TypeEnum {
        // SQLite 不区分有符号/无符号，但为了支持标准类型转换，我们识别明确标记为 UNSIGNED 的类型
        MEDIUMINT_UNSIGNED("MEDIUMINT UNSIGNED"),
        INT_UNSIGNED("INT UNSIGNED"),
        INTEGER_UNSIGNED("INTEGER UNSIGNED");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected Long merge(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            long longVal = num.longValue();
            // 处理可能的负数（JDBC可能返回负数）
            if (longVal < 0) {
                longVal = longVal & 0xFFFFFFFFL; // 转换为无符号
            }
            return Math.min(longVal, 4294967295L);
        }
        return throwUnsupportedException(val, field);
    }
}

