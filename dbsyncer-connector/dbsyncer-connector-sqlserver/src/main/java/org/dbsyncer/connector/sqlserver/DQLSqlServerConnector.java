/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver;

import org.dbsyncer.connector.sqlserver.cdc.DqlSqlServerListener;
import org.dbsyncer.connector.sqlserver.validator.DqlSqlServerConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;

/**
 * DQLSqlServer连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class DQLSqlServerConnector extends SqlServerConnector {
    
    public DQLSqlServerConnector() {
        this.isDql = true;
    }

    protected final ConfigValidator<?> configValidator = new DqlSqlServerConfigValidator();

    @Override
    public String getConnectorType() {
        return "DqlSqlServer";
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DqlSqlServerListener();
        }
        return null;
    }



}