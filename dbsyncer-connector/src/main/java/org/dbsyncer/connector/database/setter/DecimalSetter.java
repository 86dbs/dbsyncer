package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DecimalSetter extends AbstractSetter<BigDecimal> {

    @Override
    protected void set(PreparedStatement ps, int i, BigDecimal val) throws SQLException {
        ps.setBigDecimal(i, val);
    }
    
}