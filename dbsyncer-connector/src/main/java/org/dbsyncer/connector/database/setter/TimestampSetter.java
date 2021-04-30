package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TimestampSetter extends AbstractSetter<Timestamp> {

    @Override
    protected void set(PreparedStatement ps, int i, Timestamp val) throws SQLException {
        ps.setTimestamp(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedStatement ps, int i, int type, Object val) throws SQLException {
        ps.setTimestamp(i, Timestamp.valueOf(String.valueOf(val)));
    }

}