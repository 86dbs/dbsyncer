package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-25 00:03
 */
public final class OracleBytesType extends BytesType {

    @Override
    public Set<String> getSupportedTypeName() {
        // BYTES类型：用于小容量二进制数据，只支持RAW、LONG RAW、BFILE
        // BLOB类型（大容量二进制）由OracleBlobType处理
        return new HashSet<>(Arrays.asList("RAW", "LONG RAW", "BFILE"));
    }

    @Override
    protected byte[] getDefaultMergedVal() {
        return new byte[0];
    }

    @Override
    protected byte[] merge(Object val, Field field) {
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
                return null;
            }
            return s.getBytes();
        }
        return super.convert(val, field);
    }

    @Override
    protected Boolean determineIsSizeFixed(String typeName) {
        if (typeName == null) {
            return null;
        }
        
        String upperTypeName = typeName.toUpperCase();
        
        // Oracle特定类型：RAW是固定长度二进制类型
        if ("RAW".equals(upperTypeName)) {
            return true;
        }
        
        // LONG RAW、BFILE等特殊类型不适用
        if ("LONG RAW".equals(upperTypeName) || "BFILE".equals(upperTypeName)) {
            return null;
        }
        
        // 其他类型由父类处理
        return super.determineIsSizeFixed(typeName);
    }
}