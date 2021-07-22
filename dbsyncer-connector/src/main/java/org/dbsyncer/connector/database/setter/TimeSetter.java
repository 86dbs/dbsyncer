package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

public class TimeSetter extends AbstractSetter<Time> {

    @Override
    protected void set(PreparedStatement ps, int i, Time val) throws SQLException {
        ps.setTime(i, val);
    }

}