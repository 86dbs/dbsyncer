/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.schema.OracleBlobParameter;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;
import oracle.sql.BLOB;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-25 00:03
 */
public final class OracleBytesType extends BytesType {

    private enum TypeEnum {

        BLOB("BLOB"), RAW("RAW"), LONG_RAW("LONG RAW"), BFILE("BFILE");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static boolean isBlobType(String type) {
            return type != null && type.trim().toUpperCase(Locale.ROOT).contains("BLOB");
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected byte[] getDefaultMergedVal(Field field) {
        // SQL NULL 保持 null，勿归一成空数组，避免写入 EMPTY_BLOB 与校验误判
        return null;
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof OracleBlobParameter) {
            return ((OracleBlobParameter) val).getValue();
        }
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val instanceof BLOB) {
            try {
                BLOB blob = (BLOB) val;
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new OracleException(e);
            }
        }
        if (val instanceof String) {
            String s = (String) val;
            // 处理 Oracle HEXTORAW 格式: HEXTORAW('30303030303030303030303030303030')
            // 支持多种格式: HEXTORAW('...'), HEXTORAW("..."), HEXTORAW(...)
            if (s.trim().toUpperCase().startsWith("HEXTORAW(")) {
                // 提取括号内的内容
                int startIdx = s.indexOf('(');
                int endIdx = s.lastIndexOf(')');
                if (startIdx >= 0 && endIdx > startIdx) {
                    String hexContent = s.substring(startIdx + 1, endIdx).trim();
                    // 移除引号（单引号或双引号）
                    if ((hexContent.startsWith("'") && hexContent.endsWith("'")) || (hexContent.startsWith("\"") && hexContent.endsWith("\""))) {
                        hexContent = hexContent.substring(1, hexContent.length() - 1);
                    }
                    return StringUtil.hexStringToByteArray(hexContent);
                }
            }
            // 处理 EMPTY_BLOB() 格式
            if ("EMPTY_BLOB()".equalsIgnoreCase(s.trim())) {
                return new byte[0];
            }
            // 处理纯十六进制字符串（只包含 0-9, A-F, a-f）
            if (isHexString(s)) {
                return StringUtil.hexStringToByteArray(s);
            }
            // 普通字符串转换为字节数组
            return s.getBytes();
        }
        return throwUnsupportedException(val, field);
    }

    /**
     * 判断字符串是否为十六进制字符串
     */
    private boolean isHexString(String str) {
        if (StringUtil.isBlank(str)) {
            return false;
        }
        // 十六进制字符串长度必须是偶数
        if (str.length() % 2 != 0) {
            return false;
        }
        // 检查是否只包含十六进制字符
        for (char c : str.toCharArray()) {
            if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof OracleBlobParameter) {
            return val;
        }
        if (val instanceof String) {
            String s = (String) val;
            if (s.startsWith("HEXTORAW(")) {
                byte[] bytes = StringUtil.hexStringToByteArray(s.replace("HEXTORAW('", "").replace("')", ""));
                return wrapBlobIfNeeded(bytes, field);
            }
            if ("EMPTY_BLOB()".equals(s)) {
                // 空 BLOB 与 SQL NULL 区分：写入 EMPTY_BLOB，而非 setNull
                return wrapBlobIfNeeded(new byte[0], field);
            }
            return wrapBlobIfNeeded(s.getBytes(), field);
        }
        Object converted = super.convert(val, field);
        if (converted instanceof byte[]) {
            return wrapBlobIfNeeded((byte[]) converted, field);
        }
        return converted;
    }

    private Object wrapBlobIfNeeded(byte[] bytes, Field field) {
        if (TypeEnum.isBlobType(field == null ? null : field.getTypeName())) {
            return new OracleBlobParameter(bytes);
        }
        return bytes;
    }
}
