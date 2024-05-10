package org.dbsyncer.plugin.impl;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.plugin.AbstractPluginContext;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:04
 */
public final class FullPluginContext extends AbstractPluginContext {

    public FullPluginContext(ConnectorInstance sourceConnectorInstance, ConnectorInstance targetConnectorInstance, String sourceTableName, String targetTableName, String event, String pluginExtInfo) {
        super.init(sourceConnectorInstance, targetConnectorInstance, sourceTableName, targetTableName, event, null, null, pluginExtInfo);
    }

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.FULL;
    }
}