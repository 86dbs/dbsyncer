package org.dbsyncer.connector.sqlserver.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeTextType;

import java.nio.charset.StandardCharsets;

/**
 * SQL Server Unicode TEXT类型支持
 * <p>
 * SQL Server的NTEXT类型容量：
 * - NTEXT: 最大1,073,741,823 Unicode字符（已弃用，建议使用NVARCHAR(MAX)）
 * </p>
 */
public final class SqlServerUnicodeTextType extends UnicodeTextType {

    /**
     * SQL Server NTEXT类型容量常量（字符数）
     */
    private static final long NTEXT_SIZE = 1073741823L; // 2^30-1 (约1GB Unicode)

    @Override
    public java.util.Set<String> getSupportedTypeName() {
        return java.util.Collections.singleton("NTEXT");
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
        // 设置NTEXT的容量
        result.setColumnSize(NTEXT_SIZE);
        return result;
    }
}

