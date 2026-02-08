/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.StringType;

import oracle.sql.CLOB;

import oracle.sql.CLOB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-25 00:03
 */
public final class OracleStringType extends StringType {

    private enum TypeEnum {

        CHAR("CHAR"), NCHAR("NCHAR"), VARCHAR2("VARCHAR2"), NVARCHAR2("NVARCHAR2"), CLOB("CLOB"), NCLOB("NCLOB"), LONG("LONG"), ROWID("ROWID"), UROWID("UROWID");

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
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        if (val instanceof CLOB) {
            return clobToString((CLOB) val);
        }
        return throwUnsupportedException(val, field);
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