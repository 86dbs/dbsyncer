package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NCharSetter extends AbstractSetter {

    @Override
    protected void set(PreparedStatement ps, int i, Object val) throws SQLException {
        ps.setNString(i, (String) val);
    }

}