package org.dbsyncer.connector.mysql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BlobType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * MySQL BLOB类型：用于大容量二进制数据
 * 支持 TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB
 */
public final class MySQLBlobType extends BlobType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"));
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

}

