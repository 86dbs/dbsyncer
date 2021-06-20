package org.dbsyncer.connector.database;

import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;

public interface Setter {

    void set(JdbcTemplate jdbcTemplate, PreparedStatement ps, int i, int type, Object val);
    
}