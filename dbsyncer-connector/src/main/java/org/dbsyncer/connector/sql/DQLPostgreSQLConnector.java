package org.dbsyncer.connector.sql;

import org.dbsyncer.connector.postgresql.DqlPostgreSQLListener;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.PageSql;
import org.springframework.stereotype.Component;

@Component
public final class DQLPostgreSQLConnector extends AbstractDQLConnector {

    private final String TYPE = "DqlPostgreSQL";

    @Override
    public String getConnectorType() {
        return TYPE;
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return null;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DqlPostgreSQLListener();
        }
        return null;
    }

    @Override
    public String getPageSql(PageSql config) {
        return config.getQuerySql() + DatabaseConstant.POSTGRESQL_PAGE_SQL;
    }

    @Override
    public Object[] getPageArgs(ReaderConfig config) {
        int pageSize = config.getPageSize();
        int pageIndex = config.getPageIndex();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }
}