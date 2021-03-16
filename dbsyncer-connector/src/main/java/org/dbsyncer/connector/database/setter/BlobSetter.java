package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BlobSetter extends AbstractSetter<Blob> {

    @Override
    protected void set(PreparedStatement ps, int i, Blob val) throws SQLException {
        ps.setBlob(i, val);
    }

}