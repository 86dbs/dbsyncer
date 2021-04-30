package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CharSetter extends AbstractSetter<String> {

    @Override
    protected void set(PreparedStatement ps, int i, String val) throws SQLException {
        ps.setString(i, val);
    }

}