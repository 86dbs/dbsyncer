package org.dbsyncer.connector.database;

import org.dbsyncer.connector.model.PageSql;

public interface Database {

    /**
     * 获取分页SQL
     *
     * @param config
     * @return
     */
    String getPageSql(PageSql config);

    /**
     * 获取分页SQL
     *
     * @param pageIndex
     * @param pageSize
     * @return
     */
    Object[] getPageArgs(int pageIndex, int pageSize);

}