/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

import org.dbsyncer.connector.oracle.cdc.DqlOracleListener;
import org.dbsyncer.connector.oracle.validator.DqlOracleConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;

/**
 * DQLOracle连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public final class DQLOracleConnector extends OracleConnector {
    
    public DQLOracleConnector() {
        this.isDql = true;
    }


    protected final ConfigValidator<?> configValidator = new DqlOracleConfigValidator();

    @Override
    public String getConnectorType() {
        return "DqlOracle";
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DqlOracleListener();
        }
        return null;
    }



}