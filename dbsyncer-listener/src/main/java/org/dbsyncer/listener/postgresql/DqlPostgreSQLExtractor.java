package org.dbsyncer.listener.postgresql;

import org.dbsyncer.common.event.RowChangedEvent;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 22:44
 */
public class DqlPostgreSQLExtractor extends PostgreSQLExtractor {

    @Override
    public void start() {
        super.postProcessDqlBeforeInitialization();
        super.start();
    }

    @Override
    public void sendChangedEvent(RowChangedEvent event) {
        super.sendDqlChangedEvent(event);
    }
}