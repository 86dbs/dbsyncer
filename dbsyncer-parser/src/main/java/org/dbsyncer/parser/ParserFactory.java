package org.dbsyncer.parser;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.SnowflakeIdWorker;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/29 22:38
 */
@Component
public class ParserFactory implements Parser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public boolean alive(String json) {
        ConnectorConfig config;
        try {
            JSONObject configObj = new JSONObject(json);
            String connectorType = configObj.getString("connectorType");
            Class<?> configClass = ConnectorEnum.getConfigClass(connectorType);
            Object obj = JsonUtil.jsonToObj(configObj.toString(), configClass);
            Assert.notNull(obj, "ConnectorConfig is invalid.");
            config = (ConnectorConfig) obj;
            config.setConnectorType(connectorType);
            Assert.notNull(config, "ConnectorConfig can not be null.");
            return connectorFactory.isAlive(config);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }
    }

    @Override
    public MetaInfo getMetaInfo(String connectorId, String tableName) {
        ConnectorConfig config = getConnectorConfig(connectorId);
        return connectorFactory.getMetaInfo(config, tableName);
    }

    @Override
    public Connector parseConnector(String json) {
        return parseConnector(json, true);
    }

    @Override
    public Connector parseConnector(String json, boolean checkAlive) {
        // 1、Json串转Bean
        Connector connector = null;
        try {
            JSONObject conn = new JSONObject(json);
            JSONObject config = (JSONObject) conn.remove("config");
            connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);
            Assert.notNull(connector, "Connector can not be null.");
            String connectorType = config.getString("connectorType");
            Class<?> configClass = ConnectorEnum.getConfigClass(connectorType);
            ConnectorConfig obj = (ConnectorConfig) JsonUtil.jsonToObj(config.toString(), configClass);
            connector.setConfig(obj);
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }

        if (!checkAlive) {
            return connector;
        }

        // 2、验证连接是否可用
        ConnectorConfig config = connector.getConfig();
        if (!connectorFactory.isAlive(config)) {
            throw new ParserException("无法连接，请检查服务是否正常.");
        }

        // 3、config
        List<String> table = connectorFactory.getTable(config);
        connector.setTable(table);
        setConfigModel(connector, ConfigConstant.CONNECTOR);
        return connector;
    }

    @Override
    public Mapping parseMapping(String json) {
        return parseMapping(json, true);
    }

    @Override
    public Mapping parseMapping(String json, boolean checkAlive) {
        // 1、Json串转Bean
        Mapping mapping = null;
        try {
            JSONObject map = new JSONObject(json);
            JSONObject listener = (JSONObject) map.remove("listener");
            mapping = JsonUtil.jsonToObj(map.toString(), Mapping.class);
            Assert.notNull(mapping, "Mapping can not be null.");

            // 解析监听器
            String listenerType = listener.getString("listenerType");
            Class<?> configClass = ListenerEnum.getConfigClass(listenerType);
            ListenerConfig obj = (ListenerConfig) JsonUtil.jsonToObj(listener.toString(), configClass);
            mapping.setListener(obj);
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }

        if (!checkAlive) {
            return mapping;
        }

        // 2、验证连接是否可用
        aliveConnector(mapping.getSourceConnectorId());
        aliveConnector(mapping.getTargetConnectorId());

        // 3、config
        setConfigModel(mapping, ConfigConstant.MAPPING);
        return mapping;
    }

    @Override
    public TableGroup parseTableGroup(String json) {
        return parseTableGroup(json, true);
    }

    @Override
    public TableGroup parseTableGroup(String json, boolean checkAlive) {
        // 1、Json串转Bean
        TableGroup tableGroup = null;
        try {
            JSONObject conn = new JSONObject(json);
            tableGroup = JsonUtil.jsonToObj(conn.toString(), TableGroup.class);
            Assert.notNull(tableGroup, "TableGroup can not be null.");
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new ParserException(e.getMessage());
        }

        if (!checkAlive) {
            return tableGroup;
        }

        // 2、验证驱动配置是否存在
        String mappingId = tableGroup.getMappingId();
        Assert.hasText(mappingId, "MappingId can not be empty.");
        Mapping mapping = cacheService.get(mappingId, Mapping.class);
        Assert.notNull(mapping, "Mapping can not be null.");

        // 3、config
        String name = tableGroup.getName();
        tableGroup.setName(StringUtils.isEmpty(name) ? ConfigConstant.TABLE_GROUP : name);
        setConfigModel(tableGroup, ConfigConstant.TABLE_GROUP);
        return tableGroup;
    }

    @Override
    public List<OperationEnum> getOperationEnumAll() {
        return Arrays.asList(OperationEnum.values());
    }

    @Override
    public List<FilterEnum> getFilterEnumAll() {
        return Arrays.asList(FilterEnum.values());
    }

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return Arrays.asList(ConvertEnum.values());
    }

    /**
     * 验证连接器是否可用
     *
     * @param connectorId
     */
    private void aliveConnector(String connectorId) {
        ConnectorConfig config = getConnectorConfig(connectorId);
        if (!connectorFactory.isAlive(config)) {
            throw new ParserException("无法连接，请检查服务是否正常.");
        }
    }

    /**
     * 获取连接配置
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }

    private void setConfigModel(ConfigModel model, String type) {
        Assert.notNull(model, "ConfigModel can not be null.");
        Assert.hasText(type, "ConfigModel type can not be empty.");
        Assert.hasText(model.getName(), "ConfigModel name can not be empty.");

        model.setId(StringUtils.isEmpty(model.getId()) ? String.valueOf(snowflakeIdWorker.nextId()) : model.getId());
        model.setType(type);
        long now = System.currentTimeMillis();
        model.setCreateTime(null == model.getCreateTime() ? now : model.getCreateTime());
        model.setUpdateTime(now);
    }

}