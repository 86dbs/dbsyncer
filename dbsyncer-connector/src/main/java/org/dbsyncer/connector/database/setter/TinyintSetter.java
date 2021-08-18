package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TinyintSetter extends AbstractSetter<Integer> {

    @Override
    protected void set(PreparedStatement ps, int i, Integer val) throws SQLException {
        ps.setInt(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val)
            throws SQLException {
        if (val instanceof Short) {
            Short v = (Short) val;
            ps.setShort(i, v);
            return;
        }
        throw new ConnectorException(String.format("TinyintSetter can not find type [%s], val [%s]", type, val));
    }
}