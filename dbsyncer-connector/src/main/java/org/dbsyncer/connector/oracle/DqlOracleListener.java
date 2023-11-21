package org.dbsyncer.connector.oracle;

import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 22:44
 */
public class DqlOracleListener extends OracleListener {

    @Override
    public void start() {
        super.postProcessDqlBeforeInitialization();
        super.start();
    }

    @Override
    public void sendChangedEvent(ChangedEvent event) {
        super.sendDqlChangedEvent(event);
    }

    @Override
    protected Integer[] getPrimaryKeyIndexArray(List<Field> column, List<String> primaryKeys) {
        // ROW_ID
        return new Integer[]{0};
    }

}