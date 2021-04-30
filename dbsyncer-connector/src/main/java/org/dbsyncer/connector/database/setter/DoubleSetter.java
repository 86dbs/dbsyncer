package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DoubleSetter extends AbstractSetter<Double> {

    @Override
    protected void set(PreparedStatement ps, int i, Double val) throws SQLException {
        ps.setDouble(i, val);
    }

}