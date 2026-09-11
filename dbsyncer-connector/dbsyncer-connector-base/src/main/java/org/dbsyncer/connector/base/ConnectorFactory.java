/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.base;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 连接器工厂
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019-09-18 23:30
 */
@Component
public class ConnectorFactory implements DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, ConnectorInstance> pool = new ConcurrentHashMap<>();

    private final Map<String, ConnectorService> service = new ConcurrentHashMap<>();

    private final Set<String> connectorTypes = new HashSet<>();

    /**
     * 本机运行中任务占用的连接器 ID（taskId -> connectorIds）。
     */
    private final Map<String, Set<String>> runningTaskConnectors = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {
        ServiceLoader<ConnectorService> services = ServiceLoader.load(ConnectorService.class, Thread.currentThread().getContextClassLoader());
        for (ConnectorService s : services) {
            service.putIfAbsent(s.getConnectorType(), s);
            connectorTypes.add(s.getConnectorType());
        }
    }

    @Override
    public void destroy() {
        pool.values().forEach(this::disconnect);
        pool.clear();
        runningTaskConnectors.clear();
    }

    /**
     * 建立连接，返回缓存连接对象
     *
     * @param instanceId 实例ID
     * @param config 连接配置
     * @param catalog 目录
     * @param schema 模式
     */
    public ConnectorInstance connect(String instanceId, ConnectorConfig config, String catalog, String schema) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        ConnectorService connectorService = getConnectorService(config);

        DefaultConnectorServiceContext context = new DefaultConnectorServiceContext();
        context.setCatalog(catalog);
        context.setSchema(schema);
        // 创建新连接
        ConnectorInstance newInstance = connectorService.connect(config, context);
        if (newInstance == null) {
            throw new ConnectorException("连接配置异常：无法创建连接实例");
        }
        ConnectorInstance pooledInstance = pool.compute(instanceId, (k, v)-> {
            if (v != null) {
                disconnect(v);
            }
            return newInstance;
        });

        // 添加到连接池并返回克隆实例
        try {
            ConnectorInstance clone = (ConnectorInstance) pooledInstance.clone();
            clone.setConfig(config);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new ConnectorException(e);
        }
    }

    public ConnectorInstance connect(String instanceId) {
        ConnectorInstance instance = pool.get(instanceId);
        Assert.notNull(instance, "ConnectorInstance can not null");
        return instance;
    }

    /**
     * 获取监听器
     */
    public Listener getListener(String connectorType, String listenerType) {
        return getConnectorService(connectorType).getListener(listenerType);
    }

    /**
     * 检查连接配置是否可用
     */
    public boolean isAlive(String instanceId, ConnectorConfig config) {
        Assert.hasText(instanceId, "ConnectorConfigId can not be null.");
        Assert.notNull(config, "ConnectorConfig can not be null.");
        ConnectorInstance instance = findInstance(instanceId);
        if (instance != null) {
            return getConnectorService(config).isAlive(instance);
        }
        return false;
    }

    /**
     * 连接池是否已缓存该实例。
     *
     * @param instanceId 实例 ID
     * @return 已缓存返回 true
     */
    public boolean contains(String instanceId) {
        return instanceId != null && pool.containsKey(instanceId);
    }

    /**
     * 是否存在该连接器的任意缓存实例（配置级或任务级）。
     *
     * @param connectorId 连接器 ID
     * @return 存在返回 true
     */
    public boolean containsConnector(String connectorId) {
        return contains(connectorId) || hasTaskInstance(connectorId);
    }

    /**
     * 是否被本机运行中任务占用。
     *
     * @param connectorId 连接器 ID
     * @return 占用返回 true
     */
    public boolean isAcquired(String connectorId) {
        if (connectorId == null || connectorId.isEmpty()) {
            return false;
        }
        for (Set<String> ids : runningTaskConnectors.values()) {
            if (ids != null && ids.contains(connectorId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 本机任务是否已登记运行占用。
     *
     * @param taskId 任务 ID
     * @return 已登记返回 true
     */
    public boolean isTaskAcquired(String taskId) {
        return taskId != null && !taskId.isEmpty() && runningTaskConnectors.containsKey(taskId);
    }

    /**
     * 登记本机运行中任务占用的连接器。
     *
     * @param taskId       任务 ID
     * @param connectorIds 连接器 ID
     */
    public void acquire(String taskId, String... connectorIds) {
        if (taskId == null || taskId.isEmpty() || connectorIds == null) {
            return;
        }
        Set<String> set = runningTaskConnectors.computeIfAbsent(taskId, k -> ConcurrentHashMap.newKeySet());
        for (String connectorId : connectorIds) {
            if (connectorId != null && !connectorId.isEmpty()) {
                set.add(connectorId);
            }
        }
    }

    /**
     * 获取配置表
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    public List<Table> getTables(ConnectorInstance connectorInstance, ConnectorServiceContext context) {
        Assert.notNull(connectorInstance, "ConnectorInstance can not be null.");
        List<Table> tableList = getConnectorService(connectorInstance.getConfig()).getTable(connectorInstance, context);
        // 按升序展示表
        Collections.sort(tableList, Comparator.comparing(Table::getName));
        return tableList;
    }

    /**
     * 获取配置表元信息
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    public List<MetaInfo> getMetaInfo(ConnectorInstance connectorInstance, ConnectorServiceContext context) {
        Assert.notNull(connectorInstance, "ConnectorInstance can not be null.");
        return getConnectorService(connectorInstance.getConfig()).getMetaInfo(connectorInstance, context);
    }

    public Object getPosition(ConnectorInstance connectorInstance) {
        Assert.notNull(connectorInstance, "ConnectorInstance can not be null.");
        return getConnectorService(connectorInstance.getConfig()).getPosition(connectorInstance);
    }

    /**
     * 获取连接器同步参数
     *
     * @param sourceCommandConfig
     * @param targetCommandConfig
     * @return
     */
    public Map<String, String> getCommand(CommandConfig sourceCommandConfig, CommandConfig targetCommandConfig) {
        Assert.notNull(sourceCommandConfig, "SourceCommandConfig can not be null.");
        Assert.notNull(targetCommandConfig, "TargetCommandConfig can not be null.");
        Map<String, String> map = new HashMap<>();
        Map sCmd = getConnectorService(sourceCommandConfig.getConnectorType()).getSourceCommand(sourceCommandConfig);
        if (!CollectionUtils.isEmpty(sCmd)) {
            map.putAll(sCmd);
        }

        Map tCmd = getConnectorService(targetCommandConfig.getConnectorType()).getTargetCommand(targetCommandConfig);
        if (!CollectionUtils.isEmpty(tCmd)) {
            map.putAll(tCmd);
        }
        return map;
    }

    public Result reader(ReaderContext context) {
        ConnectorInstance connectorInstance = context.getSourceConnectorInstance();
        Assert.notNull(connectorInstance, "ConnectorInstance can not null");
        Assert.notNull(context, "ReaderContext can not null");
        Result result = getConnectorService(connectorInstance.getConfig()).reader(connectorInstance, context);
        Assert.notNull(result, "Connector reader result can not null");
        return result;
    }

    public Result writer(PluginContext context) {
        ConnectorInstance targetInstance = context.getTargetConnectorInstance();
        Assert.notNull(targetInstance, "targetConnectorInstance can not null");
        ConnectorService targetConnector = getConnectorService(targetInstance.getConfig());
        if (targetConnector instanceof AbstractConnector) {
            AbstractConnector conn = (AbstractConnector) targetConnector;
            try {
                conn.convertProcessBeforeWriter(context, targetConnector.getSchemaResolver());
            } catch (Exception e) {
                Result result = new Result();
                result.getError().append(e.getMessage());
                result.addFailData(context.getTargetList());
                if (context.isEnablePrintTraceInfo()) {
                    logger.error("traceId:{}, tableName:{}, event:{}, targetList:{}, result:{}", context.getTraceId(), context.getSourceTable().getName(), context.getEvent(), context
                            .getTargetList(), JsonUtil.objToJson(result));
                }
                return result;
            }
        }

        Result result = targetConnector.writer(targetInstance, context);
        if (context.isEnablePrintTraceInfo()) {
            logger.info("traceId:{}, tableName:{}, event:{}, result:{}", context.getTraceId(), context.getSourceTable().getName(), context.getEvent(), JsonUtil.objToJson(result));
        }
        Assert.notNull(result, "Connector writer batch result can not null");
        return result;
    }

    public Result writerDDL(ConnectorInstance connectorInstance, DDLConfig ddlConfig) {
        Assert.notNull(connectorInstance, "ConnectorInstance can not null");
        Result result = getConnectorService(connectorInstance.getConfig()).writerDDL(connectorInstance, ddlConfig);
        Assert.notNull(result, "Connector writer batch result can not null");
        return result;
    }

    public ConnectorService getConnectorService(ConnectorConfig connectorConfig) {
        Assert.notNull(connectorConfig, "ConnectorConfig can not null");
        return getConnectorService(connectorConfig.getConnectorType());
    }

    public ConnectorService getConnectorService(String connectorType) {
        ConnectorService connectorService = service.get(connectorType);
        if (connectorService == null) {
            Assert.isTrue(false, "Unsupported connector type:" + connectorType);
        }
        return connectorService;
    }

    public Set<String> getConnectorTypeAll() {
        return connectorTypes;
    }

    /**
     * 断开连接
     *
     * @param instanceId
     */
    public void disconnect(String instanceId) {
        ConnectorInstance instance = pool.get(instanceId);
        if (instance == null) {
            return;
        }
        // 原子性地移除实例，但不在回调中执行阻塞操作
        pool.computeIfPresent(instanceId, (k, v)->(v == instance) ? null : v);
        // 在锁外执行断开连接操作，避免阻塞其他线程
        disconnect(instance);
    }

    /**
     * 释放本机任务连接：断开该任务全部任务级实例；若无其他运行中任务占用同一连接器，再断开配置级缓存。
     *
     * @param taskId       任务 ID
     * @param connectorIds 任务用到的连接器 ID
     */
    public void releaseTask(String taskId, String... connectorIds) {
        disconnectByTaskId(taskId);
        Set<String> acquired = runningTaskConnectors.remove(taskId);
        Set<String> toCheck = new HashSet<>();
        if (acquired != null) {
            toCheck.addAll(acquired);
        }
        if (connectorIds != null) {
            for (String connectorId : connectorIds) {
                if (connectorId != null && !connectorId.isEmpty()) {
                    toCheck.add(connectorId);
                }
            }
        }
        for (String connectorId : toCheck) {
            if (!isAcquired(connectorId)) {
                disconnect(connectorId);
            }
        }
    }

    private void disconnectByTaskId(String taskId) {
        if (taskId == null || taskId.isEmpty()) {
            return;
        }
        String prefix = taskId + "@";
        List<String> keys = new ArrayList<>();
        for (String key : pool.keySet()) {
            if (key != null && key.startsWith(prefix)) {
                keys.add(key);
            }
        }
        for (String key : keys) {
            disconnect(key);
        }
    }

    private boolean hasTaskInstance(String connectorId) {
        if (connectorId == null || connectorId.isEmpty()) {
            return false;
        }
        String token = "@" + connectorId + "@";
        for (String key : pool.keySet()) {
            if (key != null && key.contains(token)) {
                return true;
            }
        }
        return false;
    }

    private ConnectorInstance findInstance(String connectorId) {
        ConnectorInstance instance = pool.get(connectorId);
        if (instance != null) {
            return instance;
        }
        String token = "@" + connectorId + "@";
        for (Map.Entry<String, ConnectorInstance> entry : pool.entrySet()) {
            if (entry.getKey() != null && entry.getKey().contains(token)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private void disconnect(ConnectorInstance connectorInstance) {
        Assert.notNull(connectorInstance, "ConnectorInstance can not be null.");
        getConnectorService(connectorInstance.getConfig()).disconnect(connectorInstance);
    }

}