/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema.support;

import oracle.sql.BLOB;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-25 00:03
 */
public final class OracleBytesType extends BytesType {

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
    protected byte[] getDefaultMergedVal() {
        return new byte[0];
    }

    @Override
    protected byte[] merge(Object val, Field field) {
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
                    if ((hexContent.startsWith("'") && hexContent.endsWith("'")) ||
                        (hexContent.startsWith("\"") && hexContent.endsWith("\""))) {
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
        if (val instanceof String) {
            String s = (String) val;
            if (s.startsWith("HEXTORAW(")) {
                return StringUtil.hexStringToByteArray(s.replace("HEXTORAW('", "").replace("')", ""));
            }
            if ("EMPTY_BLOB()".equals(s)) {
                return null;
            }
            return s.getBytes();
        }
        return super.convert(val, field);
    }
}