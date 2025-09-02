/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.config;

import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * Kafka连接配置
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-04 20:10
 */
public class KafkaConfig extends ConnectorConfig {

    private String bootstrapServers;

    // 消费者
    private String deserializer;
    private String groupId;
    private int sessionTimeoutMs;
    private int maxPartitionFetchBytes;

    // 生产者
    private String serializer;
    private int bufferMemory;
    private int batchSize;
    private int lingerMs;
    private String acks;
    private int retries;
    private int maxRequestSize;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getDeserializer() {
        return deserializer;
    }

    public void setDeserializer(String deserializer) {
        this.deserializer = deserializer;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    public void setMaxPartitionFetchBytes(int maxPartitionFetchBytes) {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    }

    public String getSerializer() {
        return serializer;
    }

    public void setSerializer(String serializer) {
        this.serializer = serializer;
    }

    public int getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public void setMaxRequestSize(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
    }
}