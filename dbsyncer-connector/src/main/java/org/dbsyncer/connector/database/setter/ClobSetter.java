package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClobSetter extends AbstractSetter<Clob> {

    @Override
    protected void set(PreparedStatement ps, int i, Clob val) throws SQLException {
        ps.setClob(i, val);
    }

}