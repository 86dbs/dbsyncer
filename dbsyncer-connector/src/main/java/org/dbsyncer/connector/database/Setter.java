package org.dbsyncer.connector.database;

import java.sql.PreparedStatement;

public interface Setter {

    void preparedStatementSetter(PreparedStatement ps, int i, int type, Object val);
    
}