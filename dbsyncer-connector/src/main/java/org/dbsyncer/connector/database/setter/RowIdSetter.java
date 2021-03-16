package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.RowId;
import java.sql.SQLException;

public class RowIdSetter extends AbstractSetter {

    @Override
    protected void set(PreparedStatement ps, int i, Object val) throws SQLException {
        ps.setRowId(i, (RowId) val);
    }

}