/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.kafka.KafkaConnector;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
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
        String consumerProperties = params.get("consumerProperties");
        String producerProperties = params.get("producerProperties");
        Assert.hasText(url, "url is empty.");
        Assert.hasText(consumerProperties, "consumerProperties is empty.");
        Assert.hasText(producerProperties, "producerProperties is empty.");

        connectorConfig.setUrl(url);
        connectorConfig.getProperties().put("consumerProperties", consumerProperties);
        connectorConfig.getProperties().put("producerProperties", producerProperties);
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