package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigintSetter extends AbstractSetter<Long> {

    @Override
    protected void set(PreparedStatement ps, int i, Long val) throws SQLException {
        ps.setLong(i, val);
    }

}