package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClobSetter extends AbstractSetter<Clob> {

    @Override
    protected void set(PreparedStatement ps, int i, Clob val) throws SQLException {
        ps.setClob(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val)
            throws SQLException {
        if (val instanceof Clob) {
            ps.setClob(i, (Clob) val);
            return;
        }
        if (val instanceof byte[]) {
            ps.setClob(i, mapper.getClob((byte[]) val));
            return;
        }
        throw new ConnectorException(String.format("ClobSetter can not find type [%s], val [%s]", type, val));
    }
}