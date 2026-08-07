/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema;

import org.dbsyncer.sdk.schema.BindParameter;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Oracle BLOB 写入绑定（须使用连接创建 LOB，MERGE 场景 setObject(byte[]) 会报 17004 无效的列类型）。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class OracleBlobParameter implements BindParameter {

    private final byte[] value;

    public OracleBlobParameter(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public void setValue(PreparedStatement ps, int paramIndex, Connection connection) throws SQLException {
        if (value == null) {
            ps.setNull(paramIndex, Types.BLOB);
            return;
        }
        Blob blob = connection.createBlob();
        if (value.length > 0) {
            blob.setBytes(1, value);
        }
        ps.setBlob(paramIndex, blob);
    }
}
