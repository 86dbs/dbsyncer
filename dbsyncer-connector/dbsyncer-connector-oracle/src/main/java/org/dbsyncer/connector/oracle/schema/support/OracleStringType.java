package org.dbsyncer.connector.oracle.schema.support;

import oracle.sql.CLOB;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Oracle字符串类型支持
 */
public final class OracleStringType extends StringType {

    private enum TypeEnum {
        VARCHAR2,  // 可变长度字符串
        NVARCHAR2, // Unicode可变长度字符串
        CHAR,      // 固定长度字符串
        NCHAR      // Unicode固定长度字符串
        // 移除了CLOB、NCLOB，因为它们有专门的DataType实现类
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
    
}