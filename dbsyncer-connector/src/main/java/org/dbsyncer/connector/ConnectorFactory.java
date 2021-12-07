package org.dbsyncer.connector;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 连接器工厂
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/18 23:30
 */
public class ConnectorFactory implements DisposableBean {

    private final Map<String, ConnectorMapper> connectorCache = new ConcurrentHashMap<>();

    @Override
    public void destroy() {
        connectorCache.values().forEach(this::disconnect);
        connectorCache.clear();
    }

    /**
     * 建立连接，返回缓存连接对象
     *
     * @param config
     */
    public ConnectorMapper connect(ConnectorConfig config) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        Connector connector = getConnector(config.getConnectorType());
        String cacheKey = connector.getConnectorMapperCacheKey(config);
        if (!connectorCache.containsKey(cacheKey)) {
            synchronized (connectorCache) {
                if (!connectorCache.containsKey(cacheKey)) {
                    connectorCache.putIfAbsent(cacheKey, connector.connect(config));
                }
            }
        }
        return connectorCache.get(cacheKey);
    }

    /**
     * 刷新连接配置
     *
     * @param config
     * @return
     */
    public boolean refresh(ConnectorConfig config) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        Connector connector = getConnector(config.getConnectorType());
        String cacheKey = connector.getConnectorMapperCacheKey(config);
        if (connectorCache.containsKey(cacheKey)) {
            disconnect(connectorCache.get(cacheKey));
            connectorCache.remove(cacheKey);
        }
        connect(config);
        return connector.isAlive(connectorCache.get(cacheKey));
    }

    /**
     * 检查连接配置是否可用
     *
     * @param config
     * @return
     */
    public boolean isAlive(ConnectorConfig config) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        Connector connector = getConnector(config.getConnectorType());
        String cacheKey = connector.getConnectorMapperCacheKey(config);
        if (connectorCache.containsKey(cacheKey)) {
            return connector.isAlive(connectorCache.get(cacheKey));
        }
        return false;
    }

    /**
     * 获取配置表
     *
     * @return
     */
    public List<Table> getTable(ConnectorMapper config) {
        Assert.notNull(config, "ConnectorMapper can not be null.");
        String type = config.getOriginalConfig().getConnectorType();
        return getConnector(type).getTable(config);
    }

    /**
     * 获取配置表元信息
     *
     * @return
     */
    public MetaInfo getMetaInfo(ConnectorMapper config, String tableName) {
        Assert.notNull(config, "ConnectorMapper can not be null.");
        Assert.hasText(tableName, "tableName can not be empty.");
        return getConnector(config).getMetaInfo(config, tableName);
    }

    /**
     * 获取连接器同步参数
     *
     * @param sourceCommandConfig
     * @param targetCommandConfig
     * @return
     */
    public Map<String, String> getCommand(CommandConfig sourceCommandConfig, CommandConfig targetCommandConfig) {
        String sType = sourceCommandConfig.getType();
        String tType = targetCommandConfig.getType();
        Map<String, String> map = new HashMap<>();
        map.putAll(getConnector(sType).getSourceCommand(sourceCommandConfig));
        map.putAll(getConnector(tType).getTargetCommand(targetCommandConfig));
        return map;
    }

    /**
     * 获取总数
     *
     * @param config
     * @param command
     * @return
     */
    public long getCount(ConnectorMapper config, Map<String, String> command) {
        return getConnector(config).getCount(config, command);
    }

    public Result reader(ConnectorMapper connectionMapper, ReaderConfig config) {
        Result result = getConnector(connectionMapper).reader(connectionMapper, config);
        Assert.notNull(result, "Connector reader result can not null");
        return result;
    }

    public Result writer(ConnectorMapper connectionMapper, WriterBatchConfig config) {
        Result result = getConnector(connectionMapper).writer(connectionMapper, config);
        Assert.notNull(result, "Connector writer batch result can not null");
        return result;
    }

    public Result writer(ConnectorMapper connectionMapper, WriterSingleConfig config) {
        Result result = getConnector(connectionMapper).writer(connectionMapper, config);
        Assert.notNull(result, "Connector writer single result can not null");
        return result;
    }

    public Connector getConnector(ConnectorMapper connectorMapper) {
        return getConnector(connectorMapper.getOriginalConfig().getConnectorType());
    }

    /**
     * 获取连接器
     *
     * @param connectorType
     * @return
     */
    private Connector getConnector(String connectorType) {
        // 获取连接器类型
        Assert.hasText(connectorType, "ConnectorType can not be empty.");
        return ConnectorEnum.getConnector(connectorType);
    }

    /**
     * 断开连接
     *
     * @param connectorMapper
     */
    private void disconnect(ConnectorMapper connectorMapper) {
        Assert.notNull(connectorMapper, "ConnectorMapper can not be null.");
        String type = connectorMapper.getOriginalConfig().getConnectorType();
        getConnector(type).disconnect(connectorMapper);
    }

}