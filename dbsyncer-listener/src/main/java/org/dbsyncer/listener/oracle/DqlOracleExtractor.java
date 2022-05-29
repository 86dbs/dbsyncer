package org.dbsyncer.listener.oracle;

import org.dbsyncer.common.event.RowChangedEvent;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 22:44
 */
public class DqlOracleExtractor extends OracleExtractor {

    @Override
    public void start() {
        super.postProcessDqlBeforeInitialization();
        super.start();
    }

    @Override
    public void changedEvent(RowChangedEvent event) {
        super.sendDqlChangedEvent(event);
    }
}
