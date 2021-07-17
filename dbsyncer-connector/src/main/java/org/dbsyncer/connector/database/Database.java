package org.dbsyncer.connector.database;

import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.config.PageSqlConfig;

public interface Database extends Connector {

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