package org.dbsyncer.sdk.model;

import org.dbsyncer.sdk.spi.ConnectorMapper;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:04
 */
public final class FullConvertContext extends AbstractConvertContext {

    public FullConvertContext(ConnectorMapper sourceConnectorMapper, ConnectorMapper targetConnectorMapper, String sourceTableName, String targetTableName, String event) {
        super.init(sourceConnectorMapper, targetConnectorMapper, sourceTableName, targetTableName, event, null, null);
    }
}