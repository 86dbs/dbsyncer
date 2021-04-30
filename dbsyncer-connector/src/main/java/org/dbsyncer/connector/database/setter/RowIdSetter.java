package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.RowId;
import java.sql.SQLException;

public class RowIdSetter extends AbstractSetter<RowId> {

    @Override
    protected void set(PreparedStatement ps, int i, RowId val) throws SQLException {
        ps.setRowId(i, val);
    }

}