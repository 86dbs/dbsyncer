/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql;

import org.dbsyncer.connector.mysql.cdc.DqlMySQLListener;
import org.dbsyncer.connector.mysql.validator.DqlMySQLConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;

/**
 * DQLMySQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public final class DQLMySQLConnector extends MySQLConnector {

    
    public DQLMySQLConnector() {
        this.isDql = true;
    }

    protected final ConfigValidator<?> configValidator = new DqlMySQLConfigValidator();

    @Override
    public String getConnectorType() {
        return "DqlMySQL";
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