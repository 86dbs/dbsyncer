/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.cdc;

import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.model.Field;

import java.util.List;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-29 22:44
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