package org.dbsyncer.listener.oracle;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.connector.model.Field;

import java.util.List;
import java.util.Set;

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
    public void sendChangedEvent(RowChangedEvent event) {
        super.sendDqlChangedEvent(event);
    }

    @Override
    protected Integer[] getPrimaryKeyIndexArray(List<Field> column, Set<String> primaryKeys) {
        // ROW_ID
        return new Integer[]{0};
    }

}