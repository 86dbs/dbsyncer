package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DateSetter extends AbstractSetter<Date> {

    @Override
    protected void set(PreparedStatement ps, int i, Date val) throws SQLException {
        ps.setDate(i, val);
    }

}