package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DecimalSetter extends AbstractSetter {

    @Override
    protected void set(PreparedStatement ps, int i, Object val) throws SQLException {
        ps.setBigDecimal(i, (BigDecimal) val);
    }
}