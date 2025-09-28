/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.connector.postgresql.cdc.DqlPostgreSQLListener;
import org.dbsyncer.connector.postgresql.validator.DqlPostgreSQLConfigValidator;
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
public final class DQLPostgreSQLConnector extends PostgreSQLConnector {

    
    public DQLPostgreSQLConnector() {
        this.isDql = true;
    }

    protected final ConfigValidator<?> configValidator = new DqlPostgreSQLConfigValidator();

    @Override
    public String getConnectorType() {
        return "DqlPostgreSQL";
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




}