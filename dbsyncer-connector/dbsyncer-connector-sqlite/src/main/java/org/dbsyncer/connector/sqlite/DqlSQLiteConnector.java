/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */

package org.dbsyncer.connector.sqlite;

import org.dbsyncer.connector.sqlite.validator.DqlSQLiteConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;

/**
 * DQLSQLite连接器实现
 *
 * @Author bble
 * @Version 1.0.0
 * @Date 2023-11-28 16:22
 */
public final class DqlSQLiteConnector extends SQLiteConnector {
    
    public DqlSQLiteConnector() {
        this.isDql = true;
    }

    protected final ConfigValidator<?> configValidator = new DqlSQLiteConfigValidator();

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



}