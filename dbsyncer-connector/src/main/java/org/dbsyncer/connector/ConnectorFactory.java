package org.dbsyncer.connector;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.sql.Connection;
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
     * 建立连接
     *
     * @param config
     */
    public Connection connect(ConnectorConfig config) {
        return connect(config, Connection.class);
    }

    public <T> T connect(ConnectorConfig config, Class<T> valueType) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        String type = config.getConnectorType();
        Connector connector = getConnector(type);
        String cacheKey = connector.getConnectorMapperCacheKey(config);
        ConnectorMapper mapper = connectorCache.get(cacheKey);
        if (null == mapper) {
            mapper = connector.connect(config);
            connectorCache.putIfAbsent(mapper.getCacheKey(), mapper);
        }
        return (T) mapper.getConnection();
    }

    /**
     * TODO 更新连接配置
     *
     * @param config
     * @return
     */
    public void updateConnectorConfig(ConnectorConfig config) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        String type = config.getConnectorType();
        Connector connector = getConnector(type);
        String cacheKey = connector.getConnectorMapperCacheKey(config);
        if (connectorCache.containsKey(cacheKey)) {
            ConnectorMapper mapper = connectorCache.get(cacheKey);
            connector.disconnect(mapper);
            connectorCache.remove(cacheKey);
        }
        connect(config);
    }

    /**
     * 检查连接配置是否可用
     *
     * @param config
     * @return
     */
    public boolean isAlive(ConnectorConfig config) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        String type = config.getConnectorType();
        Connector connector = getConnector(type);
        String cacheKey = connector.getConnectorMapperCacheKey(config);
        if (!connectorCache.containsKey(cacheKey)) {
            connect(config);
        }
        final ConnectorMapper connectorMapper = connectorCache.get(cacheKey);
        return connector.isAlive(connectorMapper);
    }

    /**
     * 获取配置表
     *
     * @return
     */
    public List<String> getTable(ConnectorConfig config) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        String type = config.getConnectorType();
        return getConnector(type).getTable(config);
    }

    /**
     * 获取配置表元信息
     *
     * @return
     */
    public MetaInfo getMetaInfo(ConnectorConfig config, String tableName) {
        Assert.notNull(config, "ConnectorConfig can not be null.");
        Assert.hasText(tableName, "tableName can not be empty.");
        String type = config.getConnectorType();
        return getConnector(type).getMetaInfo(config, tableName);
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
        Map<String, String> sCmd = getConnector(sType).getSourceCommand(sourceCommandConfig);
        Map<String, String> tCmd = getConnector(tType).getTargetCommand(targetCommandConfig);
        map.putAll(sCmd);
        map.putAll(tCmd);
        return map;
    }

    /**
     * 获取总数
     *
     * @param config
     * @param command
     * @return
     */
    public long getCount(ConnectorConfig config, Map<String, String> command) {
        Connector connector = getConnector(config.getConnectorType());
        return connector.getCount(config, command);
    }

    public Result reader(ReaderConfig config) {
        Connector connector = getConnector(config.getConfig().getConnectorType());
        Result result = connector.reader(config);
        Assert.notNull(result, "Connector reader result can not null");
        return result;
    }

    public Result writer(WriterBatchConfig config) {
        Connector connector = getConnector(config.getConfig().getConnectorType());
        Result result = connector.writer(config);
        Assert.notNull(result, "Connector writer result can not null");
        return result;
    }

    public Result writer(WriterSingleConfig config) {
        Connector connector = getConnector(config.getConfig().getConnectorType());
        Result result = connector.writer(config);
        Assert.notNull(result, "Connector writer result can not null");
        return result;
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
        String type = connectorMapper.getConfig().getConnectorType();
        getConnector(type).disconnect(connectorMapper);
    }

}