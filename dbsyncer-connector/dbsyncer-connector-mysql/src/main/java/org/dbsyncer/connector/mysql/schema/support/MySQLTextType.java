package org.dbsyncer.connector.mysql.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeTextType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL TEXT类型支持
 * <p>
 * MySQL的TEXT类型容量：
 * - TINYTEXT: 最大255字节
 * - TEXT: 最大65,535字节 (64KB)
 * - MEDIUMTEXT: 最大16,777,215字节 (16MB)
 * - LONGTEXT: 最大4,294,967,295字节 (4GB)
 * </p>
 * <p>
 * MySQL的TEXT类型默认支持UTF-8（通过字符集配置），
 * 因此标准化为UNICODE_TEXT以确保数据安全性和跨数据库兼容性。
 * </p>
 */
public final class MySQLTextType extends UnicodeTextType {

    /**
     * MySQL TEXT类型容量常量（字节数）
     */
    private static final long TINYTEXT_SIZE = 255L;
    private static final long TEXT_SIZE = 65535L;
    private static final long MEDIUMTEXT_SIZE = 16777215L;
    /**
     * LONGTEXT的实际容量：4,294,967,295字节 (4GB)
     */
    private static final long LONGTEXT_SIZE = 4294967295L;

    private enum TypeEnum {
        TINYTEXT,
        TEXT,
        MEDIUMTEXT,
        LONGTEXT
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
    public Field handleDDLParameters(ColDataType colDataType) {
        // 调用父类方法设置基础信息
        Field result = super.handleDDLParameters(colDataType);

        // 根据MySQL的TEXT类型名称设置columnSize作为标记
        // 这样在跨数据库同步时可以根据columnSize来决定使用哪种TEXT类型
        String typeName = colDataType.getDataType().toUpperCase();
        switch (typeName) {
            case "TINYTEXT":
                result.setColumnSize(TINYTEXT_SIZE);
                break;
            case "TEXT":
                result.setColumnSize(TEXT_SIZE);
                break;
            case "MEDIUMTEXT":
                result.setColumnSize(MEDIUMTEXT_SIZE);
                break;
            case "LONGTEXT":
                result.setColumnSize(LONGTEXT_SIZE);
                break;
            default:
                // 如果类型名称不在预期范围内，使用默认值TEXT
                result.setColumnSize(TEXT_SIZE);
                break;
        }

        return result;
    }
}