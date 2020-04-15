package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class DateSetter extends AbstractSetter {

    @Override
    protected void set(PreparedStatement ps, int i, Object val) throws SQLException {
        ps.setDate(i, Date.valueOf(LocalDate.parse(val + "")));
    }

}