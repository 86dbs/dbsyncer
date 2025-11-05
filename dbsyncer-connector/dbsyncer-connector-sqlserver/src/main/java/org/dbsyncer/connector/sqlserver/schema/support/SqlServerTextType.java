package org.dbsyncer.connector.sqlserver.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TextType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SQL Server TEXT类型支持
 * <p>
 * SQL Server的TEXT类型容量：
 * - TEXT: 最大2,147,483,647字符（已弃用，建议使用VARCHAR(MAX)）
 * - NTEXT: 已移至 {@link SqlServerUnicodeTextType}
 * </p>
 * <p>
 * 注意：VARCHAR 和 NVARCHAR（包括 VARCHAR(MAX) 和 NVARCHAR(MAX)）
 * 由 {@link SqlServerStringType} 处理。
 * </p>
 */
public final class SqlServerTextType extends TextType {

    /**
     * SQL Server TEXT类型容量常量（字符数）
     */
    private static final long TEXT_SIZE = 2147483647L; // 2^31-1 (约2GB)

    private enum TypeEnum {
        TEXT
        // NTEXT 已移至 SqlServerUnicodeTextType
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
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        // 调用父类方法设置基础信息
        Field result = super.handleDDLParameters(colDataType);
        // 设置TEXT的容量
        result.setColumnSize(TEXT_SIZE);
        return result;
    }
}