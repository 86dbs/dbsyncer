package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.sqlserver.ct.DqlSqlServerCTListener;
import org.dbsyncer.connector.sqlserver.validator.DqlSqlServerConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.sql.impl.SqlServerTemplate;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;

/**
 * DQL SQL Server Change Tracking (CT) 连接器实现
 */
public final class DqlSqlServerCTConnector extends SqlServerCTConnector {

    public DqlSqlServerCTConnector() {
        this.isDql = true;
    }

    protected final ConfigValidator<?> configValidator = new DqlSqlServerConfigValidator();

    @Override
    public String getConnectorType() {
        return "DqlSqlServerCT";
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            // 通过构造函数传入 SqlServerTemplate
            if (sqlTemplate instanceof SqlServerTemplate) {
                return new DqlSqlServerCTListener((SqlServerTemplate) sqlTemplate);
            }
            throw new RuntimeException("DqlSqlServerCTConnector 的 sqlTemplate 必须是 SqlServerTemplate 实例");
        }
        return null;
    }
}

