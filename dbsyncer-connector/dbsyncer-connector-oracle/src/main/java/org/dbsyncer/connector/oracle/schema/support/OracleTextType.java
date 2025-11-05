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
 * - NCLOB: 已移至 {@link OracleUnicodeTextType}
 * </p>
 */
public final class OracleTextType extends TextType {

    /**
     * Oracle CLOB类型容量常量（字节数）
     */
    private static final long CLOB_SIZE = 4294967295L; // 4GB

    private enum TypeEnum {
        CLOB
        // NCLOB 已移至 OracleUnicodeTextType
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

        // 设置CLOB的容量
        result.setColumnSize(CLOB_SIZE);

        return result;
    }
}