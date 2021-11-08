package org.dbsyncer.connector.database.setter;

import org.dbsyncer.connector.database.AbstractSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class VarBinarySetter extends AbstractSetter<byte[]> {

    @Override
    protected void set(PreparedStatement ps, int i, byte[] val) throws SQLException {
        ps.setBytes(i, val);
    }

}