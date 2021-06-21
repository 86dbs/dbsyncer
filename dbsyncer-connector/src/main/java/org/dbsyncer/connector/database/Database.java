package org.dbsyncer.connector.database;

import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.config.PageArgConfig;
import org.dbsyncer.connector.config.PageSqlBuilderConfig;
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
    String getPageSql(PageSqlBuilderConfig config);

    /**
     * 预设置分页参数
     *
     * @param sql
     * @param pageIndex
     * @param pageSize
     * @return
     */
    PageArgConfig prepareSetArgs(String sql, int pageIndex, int pageSize);

}