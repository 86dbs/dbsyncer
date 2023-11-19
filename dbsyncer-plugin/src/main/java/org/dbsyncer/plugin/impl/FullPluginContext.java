package org.dbsyncer.plugin.impl;

import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.plugin.AbstractPluginContext;
import org.dbsyncer.sdk.spi.ConnectorMapper;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:04
 */
public final class FullPluginContext extends AbstractPluginContext {

    public FullPluginContext(ConnectorMapper sourceConnectorMapper, ConnectorMapper targetConnectorMapper, String sourceTableName, String targetTableName, String event) {
        super.init(sourceConnectorMapper, targetConnectorMapper, sourceTableName, targetTableName, event, null, null);
    }

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.FULL;
    }
}