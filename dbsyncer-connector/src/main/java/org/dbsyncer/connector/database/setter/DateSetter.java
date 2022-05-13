package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class DateSetter extends AbstractSetter<Date> {

    @Override
    protected void set(PreparedStatement ps, int i, Date val) throws SQLException {
        ps.setDate(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val) throws SQLException {
        if (val instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) val;
            ps.setDate(i, Date.valueOf(timestamp.toLocalDateTime().toLocalDate()));
            return;
        }
        throw new ConnectorException(String.format("DateSetter can not find type [%s], val [%s]", type, val));
    }
}