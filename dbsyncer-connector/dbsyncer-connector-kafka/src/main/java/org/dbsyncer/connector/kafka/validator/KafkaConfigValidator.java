/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.kafka.KafkaConnector;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.util.KafkaUtil;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * Kafka连接配置校验器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
@Component
public class KafkaConfigValidator implements ConfigValidator<KafkaConnector, KafkaConfig> {

    @Override
    public void modify(KafkaConnector connectorService, KafkaConfig connectorConfig, Map<String, String> params) {
        String url = params.get("url");
        String properties = params.get("properties");
        String producerProperties = params.get(KafkaUtil.PRODUCER_PROPERTIES);
        String consumerProperties = params.get(KafkaUtil.CONSUMER_PROPERTIES);
        Assert.hasText(url, "url is empty.");
        Assert.hasText(properties, "properties is empty.");
        Assert.hasText(producerProperties, "生产者参数不能为空");
        Assert.hasText(consumerProperties, "消费者参数不能为空");
        connectorConfig.setUrl(url);
        connectorConfig.getProperties().putAll(KafkaUtil.parse(properties));
        connectorConfig.getExtInfo().put(KafkaUtil.PRODUCER_PROPERTIES, producerProperties);
        connectorConfig.getExtInfo().put(KafkaUtil.CONSUMER_PROPERTIES, consumerProperties);
    }

    @Override
    public Table modifyExtendedTable(KafkaConnector connectorService, Map<String, String> params) {
        Table table = new Table();
        String tableName = params.get("tableName");
        String columnList = params.get("columnList");
        Assert.hasText(tableName, "TableName is empty");
        Assert.hasText(columnList, "ColumnList is empty");
        List<Field> fields = JsonUtil.jsonToArray(columnList, Field.class);
        Assert.notEmpty(fields, "字段不能为空.");
        table.setName(tableName);
        table.setColumn(fields);
        table.setType(connectorService.getExtendedTableType().getCode());
        return table;
    }

}