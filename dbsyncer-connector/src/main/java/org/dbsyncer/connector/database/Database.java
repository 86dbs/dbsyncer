package org.dbsyncer.connector.database;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.model.Field;
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
     * 获取分页参数
     *
     * @param config
     * @return
     */
    Object[] getPageArgs(ReaderConfig config);

    /**
     * 获取游标分页参数
     *
     * @param config
     * @return
     */
    default Object[] getPageCursorArgs(ReaderConfig config){
        throw new ConnectorException("Unsupported override method getPageCursorArgs:" + getClass().getName());
    }

    /**
     * 获取字段名称(可重写实现系统关键字，函数名处理)
     *
     * @param field
     * @return
     */
    default String buildFieldName(Field field){
        return field.getName();
    }
}