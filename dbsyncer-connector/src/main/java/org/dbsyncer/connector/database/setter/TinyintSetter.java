package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TinyintSetter extends AbstractSetter<Integer> {

    @Override
    protected void set(PreparedStatement ps, int i, Integer val) throws SQLException {
        ps.setInt(i, val);
    }

}