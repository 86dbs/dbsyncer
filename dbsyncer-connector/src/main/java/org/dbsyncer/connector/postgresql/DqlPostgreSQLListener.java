package org.dbsyncer.connector.postgresql;

import org.dbsyncer.sdk.listener.ChangedEvent;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 22:44
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