package org.dbsyncer.common.model;

import org.dbsyncer.common.spi.ConnectorMapper;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:04
 */
public final class FullConvertContext extends AbstractConvertContext {

    public FullConvertContext(ConnectorMapper targetConnectorMapper, String sourceTableName, String targetTableName, String event,
                              List<Map> sourceList, List<Map> targetList) {
        this.targetConnectorMapper = targetConnectorMapper;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.event = event;
        this.sourceList = sourceList;
        this.targetList = targetList;
    }

}