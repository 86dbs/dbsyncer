/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.cdc;

import org.dbsyncer.sdk.listener.ChangedEvent;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-18 20:17
 */
public class DqlPostgreSQLListener extends PostgreSQLListener {

    @Override
    public void start() {
        super.postProcessDqlBeforeInitialization();
        super.start();
    }

    @Override
    public void sendChangedEvent(ChangedEvent event) {
        super.sendDqlChangedEvent(event);
    }
}