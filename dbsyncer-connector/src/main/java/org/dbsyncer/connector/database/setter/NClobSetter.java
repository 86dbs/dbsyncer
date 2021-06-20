package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NClobSetter extends AbstractSetter<NClob> {

    @Override
    protected void set(PreparedStatement ps, int i, NClob val) throws SQLException {
        ps.setNClob(i, val);
    }

    @Override
    protected void setIfValueTypeNotMatch(PreparedFieldMapper mapper, PreparedStatement ps, int i, int type, Object val) throws SQLException {
        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            NClob nClob = mapper.getNClob(bytes);
            ps.setNClob(i, nClob);
            return;
        }
        throw new ConnectorException(String.format("NClobSetter can not find type [%s], val [%s]", type, val));
    }
}