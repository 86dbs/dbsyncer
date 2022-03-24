package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.math.BigDecimal;
import java.math.BigInteger;
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
        if(val instanceof Long){
            Long l = (Long) val;
            ps.setLong(i, l);
            return;
        }
        if(val instanceof BigInteger){
            BigInteger b = (BigInteger) val;
            ps.setLong(i, new Long(b.longValue()));
            return;
        }
        throw new ConnectorException(String.format("NumericSetter can not find type [%s], val [%s]", type, val));
    }
}