package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.mysql.DqlMySQLListener;
import org.dbsyncer.sdk.quartz.DatabaseQuartzListener;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public final class DQLMySQLConnector extends AbstractDQLConnector {

    private final String TYPE = "DqlMySQL";

    @Override
    public String getConnectorType() {
        return TYPE;
    }

    @Override
    public String getPageSql(PageSql config) {
        return config.getQuerySql() + DatabaseConstant.MYSQL_PAGE_SQL;
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return super.getDqlSourceCommand(commandConfig, true);
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DqlMySQLListener();
        }
        return null;
    }
}