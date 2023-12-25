package org.dbsyncer.plugin.impl;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.plugin.AbstractPluginContext;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:06
 */
public final class IncrementPluginContext extends AbstractPluginContext {

    public IncrementPluginContext(ConnectorInstance sourceConnectorInstance, ConnectorInstance targetConnectorInstance, String sourceTableName, String targetTableName, String event, List<Map> sourceList, List<Map> targetList) {
        super.init(sourceConnectorInstance, targetConnectorInstance, sourceTableName, targetTableName, event, sourceList, targetList);
    }

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.INCREMENT;
    }
}