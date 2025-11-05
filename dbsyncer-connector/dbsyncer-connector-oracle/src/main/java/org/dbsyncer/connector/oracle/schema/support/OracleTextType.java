package org.dbsyncer.connector.oracle.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import oracle.sql.CLOB;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.TextType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Oracle TEXT类型支持
 * <p>
 * Oracle的TEXT类型容量：
 * - CLOB: 最大4GB字符数据（单字节字符集）
 * - NCLOB: 最大4GB字符数据（多字节字符集，如Unicode）
 * </p>
 */
public final class OracleTextType extends TextType {

    /**
     * Oracle CLOB/NCLOB类型容量常量（字节数）
     * 两者容量相同，但字符集支持不同
     */
    private static final long CLOB_SIZE = 4294967295L; // 4GB
    private static final long NCLOB_SIZE = 4294967295L; // 4GB

    private enum TypeEnum {
        CLOB,
        NCLOB
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
        if (val instanceof CLOB) {
            return clobToString((CLOB) val);
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        return super.convert(val, field);
    }

    public String clobToString(CLOB clob) {
        try (Reader is = clob.getCharacterStream(); BufferedReader br = new BufferedReader(is)) {
            String s = br.readLine();
            StringBuilder sb = new StringBuilder();
            while (s != null) {
                sb.append(s);
                s = br.readLine();
            }
            return sb.toString();
        } catch (SQLException | IOException e) {
            throw new OracleException(e);
        }
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        // 调用父类方法设置基础信息
        Field result = super.handleDDLParameters(colDataType);

        // 根据Oracle的TEXT类型名称设置columnSize作为标记
        // 这样在跨数据库同步时可以根据columnSize来决定使用哪种TEXT类型
        String typeName = colDataType.getDataType().toUpperCase();
        switch (typeName) {
            case "CLOB":
                result.setColumnSize(CLOB_SIZE);
                break;
            case "NCLOB":
                result.setColumnSize(NCLOB_SIZE);
                break;
            default:
                // 如果类型名称不在预期范围内，使用默认值CLOB
                result.setColumnSize(CLOB_SIZE);
                break;
        }

        return result;
    }
}