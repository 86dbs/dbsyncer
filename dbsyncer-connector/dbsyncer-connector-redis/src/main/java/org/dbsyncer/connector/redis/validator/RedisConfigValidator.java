/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.redis.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.redis.RedisConnector;
import org.dbsyncer.connector.redis.config.RedisConfig;
import org.dbsyncer.connector.redis.constant.RedisConstant;
import org.dbsyncer.connector.redis.util.RedisUtil;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * Redis连接配置校验器
 */
@Component
public class RedisConfigValidator implements ConfigValidator<RedisConnector, RedisConfig> {

    @Override
    public void modify(RedisConnector connectorService, RedisConfig connectorConfig, Map<String, String> params) {
        String url = params.get("url");
        String properties = params.get("properties");
        String password = params.get("password");
        String database = params.get("database");
        Assert.hasText(url, "url is empty.");
        Assert.hasText(properties, "properties is empty.");
        connectorConfig.setUrl(url);
        connectorConfig.setPassword(password);
        connectorConfig.setDatabase(NumberUtil.toInt(database, 0));
        connectorConfig.getProperties().putAll(RedisUtil.parse(properties));
    }

    @Override
    public Table modifyExtendedTable(RedisConnector connectorService, Map<String, String> params) {
        Table table = new Table();
        String tableName = params.get("tableName");
        String columnList = params.get("columnList");
        String groupId = params.get(RedisConstant.GROUP_ID);
        String consumerName = params.get(RedisConstant.CONSUMER_NAME);
        String stream = params.get(RedisConstant.STREAM);
        String keyPrefix = params.get(RedisConstant.KEY_PREFIX);
        String dataStructure = params.get(RedisConstant.DATA_STRUCTURE);
        String keyJoiner = params.get(RedisConstant.KEY_JOINER);
        String expireType = params.get(RedisConstant.EXPIRE_TYPE);
        String expireSeconds = params.get(RedisConstant.EXPIRE_SECONDS);
        Assert.hasText(tableName, "TableName is empty");
        Assert.hasText(columnList, "ColumnList is empty");
        List<Field> fields = JsonUtil.jsonToArray(columnList, Field.class);
        Assert.notEmpty(fields, "字段不能为空.");
        table.setName(tableName);
        table.setColumn(fields);
        table.setType(connectorService.getExtendedTableType().getCode());
        if (StringUtil.isNotBlank(stream)) {
            table.getExtInfo().put(RedisConstant.STREAM, stream.trim());
        }
        if (StringUtil.isNotBlank(keyPrefix)) {
            table.getExtInfo().put(RedisConstant.KEY_PREFIX, keyPrefix.trim());
        }
        if (StringUtil.isNotBlank(dataStructure)) {
            table.getExtInfo().put(RedisConstant.DATA_STRUCTURE, dataStructure.trim());
        }
        if (StringUtil.isNotBlank(groupId)) {
            table.getExtInfo().put(RedisConstant.GROUP_ID, groupId);
        }
        if (StringUtil.isNotBlank(consumerName)) {
            table.getExtInfo().put(RedisConstant.CONSUMER_NAME, consumerName);
        }
        if (StringUtil.isNotBlank(keyJoiner)) {
            table.getExtInfo().put(RedisConstant.KEY_JOINER, keyJoiner.trim());
        }
        if (StringUtil.isNotBlank(expireType)) {
            table.getExtInfo().put(RedisConstant.EXPIRE_TYPE, expireType.trim());
        }
        if (StringUtil.equals(expireType, RedisConstant.EXPIRE)) {
            Assert.hasText(expireSeconds, "ExpireSeconds is empty.");
            Assert.isTrue(NumberUtil.toLong(expireSeconds) > 0, "ExpireSeconds is invalid.");
            table.getExtInfo().put(RedisConstant.EXPIRE_SECONDS, expireSeconds.trim());
        }
        return table;
    }
}
