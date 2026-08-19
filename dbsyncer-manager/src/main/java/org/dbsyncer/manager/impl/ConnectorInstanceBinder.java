/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

/**
 * 任务级连接实例绑定。独立于预加载，避免与 {@link org.dbsyncer.manager.ManagerFactory} 成环。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-19
 */
@Component
public class ConnectorInstanceBinder {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    /**
     * 按 Mapping 建立源/目标连接实例。
     *
     * @param mapping 驱动
     */
    public void bind(Mapping mapping) {
        bind(mapping.getId(), mapping.getSourceConnectorId(), mapping.getSourceDatabase(), mapping.getSourceSchema(),
                mapping.getTargetConnectorId(), mapping.getTargetDatabase(), mapping.getTargetSchema());
    }

    /**
     * 按订正校验任务建立源/目标连接实例。
     *
     * @param task 校验任务
     */
    public void bind(ValidateSyncTask task) {
        bind(task.getId(), task.getSourceConnectorId(), task.getSourceDatabase(), task.getSourceSchema(),
                task.getTargetConnectorId(), task.getTargetDatabase(), task.getTargetSchema());
    }

    /**
     * 按任务 ID 建立源/目标连接实例。
     *
     * @param uniqueId           任务或 Mapping ID
     * @param sourceConnectorId  源连接器 ID
     * @param sourceDatabase     源库
     * @param sourceSchema       源 schema
     * @param targetConnectorId  目标连接器 ID
     * @param targetDatabase     目标库
     * @param targetSchema       目标 schema
     */
    public void bind(String uniqueId, String sourceConnectorId, String sourceDatabase, String sourceSchema,
                     String targetConnectorId, String targetDatabase, String targetSchema) {
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(uniqueId, sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(uniqueId, targetConnectorId, ConnectorInstanceUtil.TARGET_SUFFIX);
        Connector connector = profileComponent.getConnector(sourceConnectorId);
        ConnectorInstance instance = connectorFactory.connect(sourceInstanceId, connector.getConfig(), sourceDatabase, sourceSchema);
        Assert.notNull(instance, "Source connector instance can not null");
        connector = profileComponent.getConnector(targetConnectorId);
        instance = connectorFactory.connect(targetInstanceId, connector.getConfig(), targetDatabase, targetSchema);
        Assert.notNull(instance, "Target connector instance can not null");
    }
}
