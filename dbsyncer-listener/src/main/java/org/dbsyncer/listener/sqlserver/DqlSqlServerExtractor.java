package org.dbsyncer.listener.sqlserver;

import org.dbsyncer.common.event.ChangedEvent;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/22 22:56
 */
public class DqlSqlServerExtractor extends SqlServerExtractor {

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