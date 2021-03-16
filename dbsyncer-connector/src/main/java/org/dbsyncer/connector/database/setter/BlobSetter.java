package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BlobSetter extends AbstractSetter {

    @Override
    protected void set(PreparedStatement ps, int i, Object val) throws SQLException {
        ps.setBlob(i, (Blob) val);
    }

}