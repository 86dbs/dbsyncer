package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;

public class BitSetter extends AbstractSetter<byte[]> {

    @Override
    protected void set(PreparedStatement ps, int i, byte[] val) throws SQLException {
        ps.setBytes(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val) throws SQLException {
        if (val instanceof BitSet) {
            BitSet bitSet = (BitSet) val;
            ps.setBytes(i, bitSet.toByteArray());
            return;
        }
        if (val instanceof Integer) {
            Integer integer = (Integer) val;
            ps.setInt(i, integer);
            return;
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            ps.setBoolean(i, b);
            return;
        }
        throw new ConnectorException(String.format("BitSetter can not find type [%s], val [%s]", type, val));
    }

}