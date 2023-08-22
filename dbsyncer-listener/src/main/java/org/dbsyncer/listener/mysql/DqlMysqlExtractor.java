package org.dbsyncer.listener.mysql;

import org.dbsyncer.common.event.ChangedEvent;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/28 22:02
 */
public class DqlMysqlExtractor extends MysqlExtractor {

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