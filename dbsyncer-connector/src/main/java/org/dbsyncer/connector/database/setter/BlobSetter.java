package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BlobSetter extends AbstractSetter<Blob> {

    @Override
    protected void set(PreparedStatement ps, int i, Blob val) throws SQLException {
        ps.setBlob(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val) throws SQLException {
        // 存放jpg等文件
        if (val instanceof Blob) {
            Blob blob = (Blob) val;
            ps.setBlob(i, blob);
            return;
        }
        throw new ConnectorException(String.format("BlobSetter can not find type [%s], val [%s]", type, val));
    }

}