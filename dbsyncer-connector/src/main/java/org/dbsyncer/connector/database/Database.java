package org.dbsyncer.connector.database;

import org.dbsyncer.connector.config.ReaderConfig;
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
     * 获取分页游标SQL
     *
     * @param pageSql
     * @return
     */
    default String getPageCursorSql(PageSql pageSql) {
        return "";
    }

    /**
     * 获取分页SQL
     *
     * @param config
     * @return
     */
    Object[] getPageArgs(ReaderConfig config);

}