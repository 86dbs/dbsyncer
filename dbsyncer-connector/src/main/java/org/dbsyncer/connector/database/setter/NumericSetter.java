package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NumericSetter extends AbstractSetter<BigDecimal> {

    @Override
    protected void set(PreparedStatement ps, int i, BigDecimal val) throws SQLException {
        ps.setBigDecimal(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val) throws SQLException {
        if(val instanceof Integer){
            Integer integer = (Integer) val;
            ps.setInt(i, integer);
            return;
        }
        throw new ConnectorException(String.format("NumericSetter can not find type [%s], val [%s]", type, val));
    }
}