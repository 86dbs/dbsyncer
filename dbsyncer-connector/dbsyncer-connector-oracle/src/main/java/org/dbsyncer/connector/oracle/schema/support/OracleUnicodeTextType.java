package org.dbsyncer.connector.oracle.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import oracle.sql.CLOB;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.UnicodeTextType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

/**
 * Oracle Unicode TEXT类型支持
 * <p>
 * Oracle的NCLOB类型容量：
 * - NCLOB: 最大4GB字符数据（多字节字符集，如Unicode）
 * </p>
 */
public final class OracleUnicodeTextType extends UnicodeTextType {

    /**
     * Oracle NCLOB类型容量常量（字节数）
     */
    private static final long NCLOB_SIZE = 4294967295L; // 4GB

    @Override
    public java.util.Set<String> getSupportedTypeName() {
        return java.util.Collections.singleton("NCLOB");
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
        // 设置NCLOB的容量
        result.setColumnSize(NCLOB_SIZE);
        return result;
    }
}

