package org.dbsyncer.connector.database;

import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageSqlConfig;
import org.springframework.jdbc.core.JdbcTemplate;

public interface Database extends Connector {

    JdbcTemplate getJdbcTemplate(DatabaseConfig config);

    void close(JdbcTemplate jdbcTemplate);

    /**
     * 获取分页SQL
     *
     * @param config
     * @return
     */
    String getPageSql(PageSqlConfig config);

    /**
     * 获取分页SQL
     *
     * @param pageIndex
     * @param pageSize
     * @return
     */
    Object[] getPageArgs(int pageIndex, int pageSize);

}