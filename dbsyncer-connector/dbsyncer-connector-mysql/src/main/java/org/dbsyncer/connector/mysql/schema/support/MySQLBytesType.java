/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLBytesType extends BytesType {

    @Override
    public Set<String> getSupportedTypeName() {
        // BYTES类型：用于小容量二进制数据，只支持BINARY和VARBINARY
        // BLOB类型（TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB）由MySQLBlobType处理
        return new HashSet<>(Arrays.asList("BINARY", "VARBINARY"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof String) {
            return ((String) val).getBytes(StandardCharsets.UTF_8);
        }
        if (val instanceof BitSet) {
            BitSet bitSet = (BitSet) val;
            return bitSet.toByteArray();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Boolean determineIsSizeFixed(String typeName) {
        if (typeName == null) {
            return null;
        }
        
        String upperTypeName = typeName.toUpperCase();
        
        // MySQL固定长度类型：BINARY
        if ("BINARY".equals(upperTypeName)) {
            return true;
        }
        
        // MySQL可变长度类型：VARBINARY
        if ("VARBINARY".equals(upperTypeName)) {
            return false;
        }
        
        return null;
    }

}