package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LongVarcharSetter extends AbstractSetter<String> {

    @Override
    protected void set(PreparedStatement ps, int i, String val) throws SQLException {
        ps.setString(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedStatement ps, int i, int type, Object val) throws SQLException {
        // TODO mysql5.7 text
        ps.setString(i, String.valueOf(val));
    }
}