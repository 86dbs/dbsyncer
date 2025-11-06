package org.dbsyncer.connector.sqlite.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnsignedDecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQLite 无符号精确小数类型支持
 * <p>
 * SQLite 不区分有符号/无符号，所有小数类型都映射到 REAL 存储类。
 * 虽然 SQLite 不支持真正的 UNSIGNED 语法，但在从其他数据库同步表结构时，
 * DDL 可能保留原始类型定义（如 "DECIMAL UNSIGNED"），因此我们识别这些类型名
 * 以尊重原始 DDL 定义。实际存储仍为 REAL，但类型名会保留。
 * </p>
 * <p>
 * <b>类型分析：</b>
 * <ul>
 *   <li><b>DECIMAL UNSIGNED</b> - 识别此类型名以尊重从其他数据库同步时的原始 DDL 定义。
 *       虽然 SQLite 实际存储为 REAL，但类型名会保留为 "DECIMAL UNSIGNED"。</li>
 * </ul>
 * </p>
 */
public final class SQLiteUnsignedDecimalType extends UnsignedDecimalType {

    private enum TypeEnum {
        // SQLite 不区分有符号/无符号，但为了支持标准类型转换，我们识别明确标记为 UNSIGNED 的类型
        DECIMAL_UNSIGNED("DECIMAL UNSIGNED");

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
    protected BigDecimal merge(Object val, Field field) {
        if (val instanceof Number) {
            BigDecimal bd = new BigDecimal(val.toString());
            // 确保值 >= 0
            if (bd.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return bd;
        }
        return throwUnsupportedException(val, field);
    }
}

