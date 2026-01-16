/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.cdc;

import org.dbsyncer.sdk.listener.ChangedEvent;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-29 22:44
 */
public class DqlOracleListener extends OracleListener {

    @Override
    public void start() throws Exception {
        super.postProcessDqlBeforeInitialization();
        super.start();
    }
}