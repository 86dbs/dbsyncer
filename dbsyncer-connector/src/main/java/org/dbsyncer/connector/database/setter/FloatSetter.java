package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FloatSetter extends AbstractSetter<Float> {

    @Override
    protected void set(PreparedStatement ps, int i, Float val) throws SQLException {
        ps.setFloat(i, val);
    }

}