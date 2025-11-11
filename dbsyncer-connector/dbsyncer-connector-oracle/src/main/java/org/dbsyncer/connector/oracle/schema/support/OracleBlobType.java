package org.dbsyncer.connector.oracle.schema.support;

import oracle.sql.BLOB;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BlobType;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Oracle BLOB类型：用于大容量二进制数据
 * 支持 BLOB 类型
 */
public final class OracleBlobType extends BlobType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("BLOB"));
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof BLOB) {
            try {
                BLOB blob = (BLOB) val;
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new OracleException(e);
            }
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            String s = (String) val;
            if ("EMPTY_BLOB()".equals(s)) {
                return null;
            }
            return s.getBytes();
        }
        return super.convert(val, field);
    }
}

