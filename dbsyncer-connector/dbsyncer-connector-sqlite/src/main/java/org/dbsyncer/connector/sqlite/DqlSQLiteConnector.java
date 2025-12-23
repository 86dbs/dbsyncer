/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */

package org.dbsyncer.connector.sqlite;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.plugin.ReaderContext;

import java.util.List;

/**
 * DQLSQLite连接器实现
 *
 * @Author bble
 * @Version 1.0.0
 * @Date 2023-11-28 16:22
 */
@Deprecated
public final class DqlSQLiteConnector extends AbstractDQLConnector {

    @Override
    public String getConnectorType() {
        return "DqlSQLite";
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }
        return null;
    }

    @Override
    public String getPageSql(PageSql config) {
        List<String> primaryKeys = config.getPrimaryKeys();
        String orderBy = StringUtil.join(primaryKeys, StringUtil.COMMA);
        return String.format(DatabaseConstant.SQLITE_PAGE_SQL, orderBy, config.getQuerySql());
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[] {(pageIndex - 1) * pageSize + 1, pageIndex * pageSize};
    }
}