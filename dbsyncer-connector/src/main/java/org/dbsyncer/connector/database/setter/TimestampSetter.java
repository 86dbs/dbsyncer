package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class TimestampSetter extends AbstractSetter<Timestamp> {

    @Override
    protected void set(PreparedStatement ps, int i, Timestamp val) throws SQLException {
        ps.setTimestamp(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val) throws SQLException {
        if(val instanceof Date){
            Date date = (Date) val;
            ps.setTimestamp(i, new Timestamp(date.getTime()));
            return;
        }

        if (val instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) val;
            ps.setTimestamp(i, Timestamp.valueOf(dateTime));
            return;
        }

        if (val instanceof String) {
            String s = (String) val;
            ps.setTimestamp(i, Timestamp.valueOf(s));
            return;
        }

        throw new ConnectorException(String.format("TimestampSetter can not find type [%s], val [%s]", type, val));
    }

}