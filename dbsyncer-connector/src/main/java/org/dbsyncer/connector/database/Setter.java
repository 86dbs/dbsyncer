package org.dbsyncer.connector.database;

import java.sql.Connection;
import java.sql.PreparedStatement;

public interface Setter {

    void set(Connection connection, PreparedStatement ps, int i, int type, Object val);
    
}