package org.dbsyncer.connector.database;

import java.sql.PreparedStatement;

public interface Setter {

    void set(PreparedStatement ps, int i, int type, Object val);
    
}