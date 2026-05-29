/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.redis;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.redis.cdc.RedisListener;
import org.dbsyncer.connector.redis.config.RedisConfig;
import org.dbsyncer.connector.redis.schema.RedisSchemaResolver;
import org.dbsyncer.connector.redis.util.RedisUtil;
import org.dbsyncer.connector.redis.validator.RedisConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.connector.AbstractConnector;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Redis连接器实现
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:20
 */
public class RedisConnector extends AbstractConnector implements ConnectorService<RedisConnectorInstance, RedisConfig> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String KEY_PREFIX = "keyPrefix";

    private final RedisConfigValidator configValidator = new RedisConfigValidator();
    private final RedisSchemaResolver schemaResolver = new RedisSchemaResolver();

    @Override
    public String getConnectorType() {
        return "Redis";
    }

    @Override
    public TableTypeEnum getExtendedTableType() {
        return TableTypeEnum.SEMI;
    }

    @Override
    public Class<RedisConfig> getConfigClass() {
        return RedisConfig.class;
    }

    @Override
    public ConnectorInstance connect(RedisConfig config, ConnectorServiceContext context) {
        return new RedisConnectorInstance(config);
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public void disconnect(RedisConnectorInstance connectorInstance) {
        connectorInstance.close();
    }

    @Override
    public boolean isAlive(RedisConnectorInstance connectorInstance) {
        try {
            connectorInstance.ping();
            return true;
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            throw new RedisException(e);
        }
    }

    @Override
    public List<String> getDatabases(RedisConnectorInstance connectorInstance) {
        List<String> list = new ArrayList<>();
        list.add("db" + connectorInstance.getConfig().getDatabase());
        return list;
    }

    @Override
    public List<Table> getTable(RedisConnectorInstance connectorInstance, ConnectorServiceContext context) {
        return new ArrayList<>();
    }

    @Override
    public List<MetaInfo> getMetaInfo(RedisConnectorInstance connectorInstance, ConnectorServiceContext context) {
        List<MetaInfo> metaInfos = new ArrayList<>();
        for (Table table : context.getTablePatterns()) {
            MetaInfo metaInfo = new MetaInfo();
            metaInfo.setTable(table.getName());
            metaInfo.setTableType(getExtendedTableType().getCode());
            metaInfo.setColumn(table.getColumn());
            metaInfo.setExtInfo(table.getExtInfo());
            metaInfos.add(metaInfo);
        }
        return metaInfos;
    }

    @Override
    public long getCount(RedisConnectorInstance connectorInstance, MetaContext metaContext) {
        return 0;
    }

    @Override
    public Result reader(RedisConnectorInstance connectorInstance, ReaderContext context) {
        throw new RedisException("Redis暂不支持全量读取");
    }

    @Override
    public Result writer(RedisConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        if (CollectionUtils.isEmpty(data)) {
            throw new RedisException("writer data can not be empty.");
        }

        Result result = new Result();
        final List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        String keyPrefix = context.getCommand().get(KEY_PREFIX);
        String event = context.getEvent();
        Jedis jedis = null;
        try {
            jedis = connectorInstance.borrowJedis();
            for (Map row : data) {
                String redisKey = buildRedisKey(keyPrefix, pkFields, row);
                if (StringUtil.equals(event, ConnectorConstant.OPERTION_DELETE)) {
                    jedis.del(redisKey);
                } else {
                    jedis.set(redisKey, JsonUtil.objToJson(row));
                }
            }
            result.addSuccessData(data);
        } catch (Exception e) {
            result.addFailData(data);
            result.getError().append(e.getMessage()).append(System.lineSeparator());
            logger.error(e.getMessage(), e);
        } finally {
            RedisUtil.returnResource(connectorInstance.getConnection(), jedis);
        }
        return result;
    }

    private String buildRedisKey(String keyPrefix, List<Field> pkFields, Map row) {
        List<String> parts = pkFields.stream()
                .map(f -> String.valueOf(row.get(f.getName())))
                .collect(Collectors.toList());
        String key = StringUtil.join(parts, StringUtil.UNDERLINE);
        if (StringUtil.isBlank(keyPrefix)) {
            return key;
        }
        return keyPrefix + StringUtil.COLON + key;
    }

    @Override
    public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
        return new HashMap<>();
    }

    @Override
    public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
        Map<String, String> cmd = new HashMap<>();
        cmd.put(KEY_PREFIX, commandConfig.getTable().getName());
        return cmd;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isLog(listenerType)) {
            return new RedisListener();
        }
        return null;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }
}
