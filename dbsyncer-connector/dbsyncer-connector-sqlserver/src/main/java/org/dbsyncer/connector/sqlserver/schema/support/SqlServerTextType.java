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
 * - NTEXT: 最大1,073,741,823 Unicode字符（已弃用，建议使用NVARCHAR(MAX)）
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
    private static final long NTEXT_SIZE = 1073741823L; // 2^30-1 (约1GB Unicode)

    private enum TypeEnum {
        TEXT,
        NTEXT
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

        // 根据SQL Server的TEXT类型名称设置columnSize作为标记
        String typeName = colDataType.getDataType().toUpperCase();
        switch (typeName) {
            case "TEXT":
                result.setColumnSize(TEXT_SIZE);
                break;
            case "NTEXT":
                result.setColumnSize(NTEXT_SIZE);
                break;
            default:
                // 如果类型名称不在预期范围内，使用默认值TEXT
                result.setColumnSize(TEXT_SIZE);
                break;
        }

        return result;
    }
}