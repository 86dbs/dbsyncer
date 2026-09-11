/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.spi.ClusterService;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

/**
 * 任务级连接实例绑定。独立于预加载，避免与 {@link org.dbsyncer.manager.ManagerFactory} 成环。
 * <p>集群：{@link #restore} 在启任务前按负责任务恢复连接；{@link #release} 在停任务后按运行中引用回收。
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

    @Resource
    private ClusterService clusterService;

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
     * @param uniqueId          任务或 Mapping ID
     * @param sourceConnectorId 源连接器 ID
     * @param sourceDatabase    源库
     * @param sourceSchema      源 schema
     * @param targetConnectorId 目标连接器 ID
     * @param targetDatabase    目标库
     * @param targetSchema      目标 schema
     */
    public void bind(String uniqueId, String sourceConnectorId, String sourceDatabase, String sourceSchema,
                     String targetConnectorId, String targetDatabase, String targetSchema) {
        connect(uniqueId, sourceConnectorId, sourceDatabase, sourceSchema, ConnectorInstanceUtil.SOURCE_SUFFIX);
        connect(uniqueId, targetConnectorId, targetDatabase, targetSchema, ConnectorInstanceUtil.TARGET_SUFFIX);
    }

    /**
     * 集群：按负责任务恢复连接并登记运行占用（半失败会回滚已建连）。
     *
     * @param mapping 驱动
     */
    public void restore(Mapping mapping) {
        if (mapping == null || clusterService.isStandalone()) {
            return;
        }
        try {
            bind(mapping);
            connectorFactory.acquire(mapping.getId(), mapping.getSourceConnectorId(), mapping.getTargetConnectorId());
        } catch (RuntimeException e) {
            connectorFactory.releaseTask(mapping.getId(), mapping.getSourceConnectorId(), mapping.getTargetConnectorId());
            throw e;
        }
    }

    /**
     * 集群：释放本机该任务连接；无其他运行中任务占用同一连接器时断开配置级缓存。
     *
     * @param mapping 驱动
     */
    public void release(Mapping mapping) {
        if (mapping == null || clusterService.isStandalone()) {
            return;
        }
        connectorFactory.releaseTask(mapping.getId(), mapping.getSourceConnectorId(), mapping.getTargetConnectorId());
    }

    /**
     * 确保任务级连接存在（页面按需建连，不登记运行占用）。
     *
     * @param mapping 驱动
     * @param suffix  源/目标后缀
     * @return 连接实例
     */
    public ConnectorInstance ensure(Mapping mapping, String suffix) {
        boolean source = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        return ensure(mapping.getId(),
                source ? mapping.getSourceConnectorId() : mapping.getTargetConnectorId(),
                source ? mapping.getSourceDatabase() : mapping.getTargetDatabase(),
                source ? mapping.getSourceSchema() : mapping.getTargetSchema(),
                suffix);
    }

    /**
     * 确保任务级连接存在（页面按需建连，不登记运行占用）。
     *
     * @param task   校验任务
     * @param suffix 源/目标后缀
     * @return 连接实例
     */
    public ConnectorInstance ensure(ValidateSyncTask task, String suffix) {
        boolean source = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        return ensure(task.getId(),
                source ? task.getSourceConnectorId() : task.getTargetConnectorId(),
                source ? task.getSourceDatabase() : task.getTargetDatabase(),
                source ? task.getSourceSchema() : task.getTargetSchema(),
                suffix);
    }

    private ConnectorInstance ensure(String uniqueId, String connectorId, String database, String schema, String suffix) {
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(uniqueId, connectorId, suffix);
        if (connectorFactory.contains(instanceId)) {
            return connectorFactory.connect(instanceId);
        }
        return connect(uniqueId, connectorId, database, schema, suffix);
    }

    private ConnectorInstance connect(String uniqueId, String connectorId, String database, String schema, String suffix) {
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(uniqueId, connectorId, suffix);
        Connector connector = profileComponent.getConnector(connectorId);
        Assert.notNull(connector, "连接器不存在");
        ConnectorInstance instance = connectorFactory.connect(instanceId, connector.getConfig(), database, schema);
        Assert.notNull(instance, "Connector instance can not null");
        return instance;
    }
}
