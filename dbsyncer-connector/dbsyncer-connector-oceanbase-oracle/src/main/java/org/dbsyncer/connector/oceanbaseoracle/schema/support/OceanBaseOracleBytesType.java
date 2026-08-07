/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbaseoracle.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oceanbaseoracle.OceanBaseOracleException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * OceanBase Oracle 模式二进制类型（标准 JDBC Blob）
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 14:10
 */
public final class OceanBaseOracleBytesType extends BytesType {

    private enum TypeEnum {
        BLOB("BLOB"),
        RAW("RAW"),
        LONG_RAW("LONG RAW"),
        BFILE("BFILE");

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
    protected byte[] getDefaultMergedVal(Field field) {
        return null;
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val instanceof Blob) {
            try {
                Blob blob = (Blob) val;
                long length = blob.length();
                if (length <= 0) {
                    return new byte[0];
                }
                return blob.getBytes(1, (int) length);
            } catch (SQLException e) {
                throw new OceanBaseOracleException(e);
            }
        }
        if (val instanceof String) {
            String s = (String) val;
            if ("EMPTY_BLOB()".equalsIgnoreCase(s.trim())) {
                return new byte[0];
            }
            if (s.trim().toUpperCase().startsWith("HEXTORAW(")) {
                int startIdx = s.indexOf('(');
                int endIdx = s.lastIndexOf(')');
                if (startIdx >= 0 && endIdx > startIdx) {
                    String hexContent = s.substring(startIdx + 1, endIdx).trim();
                    if ((hexContent.startsWith("'") && hexContent.endsWith("'"))
                            || (hexContent.startsWith("\"") && hexContent.endsWith("\""))) {
                        hexContent = hexContent.substring(1, hexContent.length() - 1);
                    }
                    return StringUtil.hexStringToByteArray(hexContent);
                }
            }
            return s.getBytes();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            String s = (String) val;
            if (s.startsWith("HEXTORAW(")) {
                return StringUtil.hexStringToByteArray(s.replace("HEXTORAW('", "").replace("')", ""));
            }
            if ("EMPTY_BLOB()".equals(s)) {
                return new byte[0];
            }
            return s.getBytes();
        }
        return super.convert(val, field);
    }
}
