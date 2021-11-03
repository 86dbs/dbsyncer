package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IntegerSetter extends AbstractSetter<Integer> {

    @Override
    protected void set(PreparedStatement ps, int i, Integer val) throws SQLException {
        ps.setInt(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val)
            throws SQLException {
        if (val instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) val;
            ps.setInt(i, bigInteger.intValue());
            return;
        }

        if (val instanceof Long) {
            Long l = (Long) val;
            ps.setInt(i, l.intValue());
            return;
        }

        if (val instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) val;
            ps.setInt(i, bigDecimal.intValue());
            return;
        }

        throw new ConnectorException(String.format("IntegerSetter can not find type [%s], val [%s]", type, val));
    }
}