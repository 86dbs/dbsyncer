package org.dbsyncer.common.model;

import org.dbsyncer.common.spi.ConnectorMapper;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:06
 */
public final class IncrementConvertContext extends AbstractConvertContext {

    public IncrementConvertContext(ConnectorMapper sourceConnectorMapper, ConnectorMapper targetConnectorMapper, String sourceTableName, String targetTableName, String event, List<Map> sourceList, List<Map> targetList) {
        super.init(sourceConnectorMapper, targetConnectorMapper, sourceTableName, targetTableName, event, sourceList, targetList);
    }
}