package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigintSetter extends AbstractSetter<Long> {

    @Override
    protected void set(PreparedStatement ps, int i, Long val) throws SQLException {
        ps.setLong(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val)
            throws SQLException {
        if (val instanceof BigDecimal) {
            BigDecimal bitDec = (BigDecimal) val;
            ps.setLong(i, bitDec.longValue());
            return;
        }
        if (val instanceof BigInteger) {
            BigInteger bitInt = (BigInteger) val;
            ps.setLong(i, bitInt.longValue());
            return;
        }
        throw new ConnectorException(String.format("BigintSetter can not find type [%s], val [%s]", type, val));
    }
}