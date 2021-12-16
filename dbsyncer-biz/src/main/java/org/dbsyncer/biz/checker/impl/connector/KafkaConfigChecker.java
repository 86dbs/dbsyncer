package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.connector.config.KafkaConfig;
import org.dbsyncer.connector.enums.KafkaSerializerTypeEnum;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/22 23:55
 */
@Component
public class KafkaConfigChecker implements ConnectorConfigChecker<KafkaConfig> {

    @Override
    public void modify(KafkaConfig connectorConfig, Map<String, String> params) {
        String bootstrapServers = params.get("bootstrapServers");
        String serializer = params.get("serializer");
        String topic = params.get("topic");
        String fields = params.get("fields");
        Assert.hasText(bootstrapServers, "bootstrapServers is empty.");
        Assert.hasText(serializer, "serializer is empty.");
        Assert.hasText(topic, "topic is empty.");
        Assert.hasText(fields, "fields is empty.");
        Assert.notNull(KafkaSerializerTypeEnum.getSerializerType(serializer), "Unsupported serializer type");

        String groupId = params.get("groupId");
        Assert.hasText(groupId, "groupId is empty.");
        int sessionTimeoutMs = NumberUtil.toInt(params.get("sessionTimeoutMs"));
        int maxPartitionFetchBytes = NumberUtil.toInt(params.get("maxPartitionFetchBytes"));

        String acks = params.get("acks");
        Assert.hasText(acks, "acks is empty.");
        int bufferMemory = NumberUtil.toInt(params.get("bufferMemory"));
        int batchSize = NumberUtil.toInt(params.get("batchSize"));
        int lingerMs = NumberUtil.toInt(params.get("lingerMs"));
        int retries = NumberUtil.toInt(params.get("retries"));
        int maxRequestSize = NumberUtil.toInt(params.get("maxRequestSize"));

        connectorConfig.setBootstrapServers(bootstrapServers);
        connectorConfig.setSerializer(serializer);
        connectorConfig.setTopic(topic);
        connectorConfig.setFields(fields);

        connectorConfig.setGroupId(groupId);
        connectorConfig.setSessionTimeoutMs(sessionTimeoutMs);
        connectorConfig.setMaxPartitionFetchBytes(maxPartitionFetchBytes);

        connectorConfig.setBufferMemory(bufferMemory);
        connectorConfig.setBatchSize(batchSize);
        connectorConfig.setLingerMs(lingerMs);
        connectorConfig.setAcks(acks);
        connectorConfig.setRetries(retries);
        connectorConfig.setMaxRequestSize(maxRequestSize);
    }

}
