/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.connector.config.KafkaConfig;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * Kafka连接配置校验器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
@Component
public class KafkaConfigValidator implements ConfigValidator<KafkaConfig> {

    @Override
    public void modify(KafkaConfig connectorConfig, Map<String, String> params) {
        String bootstrapServers = params.get("bootstrapServers");
        String topic = params.get("topic");
        String fields = params.get("fields");
        Assert.hasText(bootstrapServers, "bootstrapServers is empty.");
        Assert.hasText(topic, "topic is empty.");
        Assert.hasText(fields, "fields is empty.");

        String groupId = params.get("groupId");
        String serializer = params.get("serializer");
        Assert.hasText(groupId, "groupId is empty.");
        Assert.hasText(serializer, "serializer is empty.");
        int sessionTimeoutMs = NumberUtil.toInt(params.get("sessionTimeoutMs"));
        int maxPartitionFetchBytes = NumberUtil.toInt(params.get("maxPartitionFetchBytes"));

        String deserializer = params.get("deserializer");
        String acks = params.get("acks");
        Assert.hasText(deserializer, "deserializer is empty.");
        Assert.hasText(acks, "acks is empty.");
        int bufferMemory = NumberUtil.toInt(params.get("bufferMemory"));
        int batchSize = NumberUtil.toInt(params.get("batchSize"));
        int lingerMs = NumberUtil.toInt(params.get("lingerMs"));
        int retries = NumberUtil.toInt(params.get("retries"));
        int maxRequestSize = NumberUtil.toInt(params.get("maxRequestSize"));

        connectorConfig.setBootstrapServers(bootstrapServers);
        connectorConfig.setTopic(topic);
        connectorConfig.setFields(fields);

        connectorConfig.setGroupId(groupId);
        connectorConfig.setSerializer(serializer);
        connectorConfig.setSessionTimeoutMs(sessionTimeoutMs);
        connectorConfig.setMaxPartitionFetchBytes(maxPartitionFetchBytes);

        connectorConfig.setDeserializer(deserializer);
        connectorConfig.setBufferMemory(bufferMemory);
        connectorConfig.setBatchSize(batchSize);
        connectorConfig.setLingerMs(lingerMs);
        connectorConfig.setAcks(acks);
        connectorConfig.setRetries(retries);
        connectorConfig.setMaxRequestSize(maxRequestSize);
    }

}
