package org.dbsyncer.connector.database;

import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.springframework.jdbc.core.JdbcTemplate;

public interface Database extends Connector {

    String getPageSql(String tableName, String pk, String querySQL);

    JdbcTemplate getJdbcTemplate(DatabaseConfig config);

    void close(JdbcTemplate jdbcTemplate);

}