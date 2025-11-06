package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedByteType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite 无符号字节类型支持
 * <p>
 * SQLite 不区分有符号/无符号，所有整数类型都映射到 INTEGER 存储类。
 * 虽然 SQLite 不支持真正的 UNSIGNED 语法，但在从其他数据库同步表结构时，
 * DDL 可能保留原始类型定义（如 "TINYINT UNSIGNED"），因此我们识别这些类型名
 * 以尊重原始 DDL 定义。实际存储仍为 INTEGER，但类型名会保留。
 * </p>
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>TINYINT UNSIGNED</b> - 识别此类型名以尊重从其他数据库同步时的原始 DDL 定义。
 *       虽然 SQLite 实际存储为 INTEGER，但类型名会保留为 "TINYINT UNSIGNED"。</li>
 * </ul>
 * </p>
 */
public final class SQLiteUnsignedByteType extends UnsignedByteType {

    private enum TypeEnum {
        // SQLite 不区分有符号/无符号，但为了支持标准类型转换，我们识别明确标记为 UNSIGNED 的类型
        TINYINT_UNSIGNED("TINYINT UNSIGNED");

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
    protected Short merge(Object val, Field field) {
        if (val instanceof Number) {
            Number num = (Number) val;
            int intVal = num.intValue();
            // 处理可能的负数（JDBC可能返回负数）
            if (intVal < 0) {
                intVal = intVal & 0xFF; // 转换为无符号
            }
            return (short) Math.min(intVal, 255);
        }
        return throwUnsupportedException(val, field);
    }
}

