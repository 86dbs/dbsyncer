/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.rocketmq;

import org.dbsyncer.connector.rocketmq.config.RocketMQConfig;
import org.dbsyncer.connector.rocketmq.util.RocketMQUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 01:00
 */
public final class RocketMQConnectorInstance implements ConnectorInstance<RocketMQConfig, DefaultMQProducer> {

    private RocketMQConfig config;
    private DefaultMQProducer producer;

    public RocketMQConnectorInstance(RocketMQConfig config) {
        this.config = config;
        try {
            producer = RocketMQUtil.createProducer(config);
            ping();
        } catch (Exception e) {
            close();
            throw new RocketMQException("创建 RocketMQ Producer 失败，URL: " + config.getUrl() + "，错误: " + e.getMessage(), e);
        }
    }

    @Override
    public String getServiceUrl() {
        return config.getUrl();
    }

    @Override
    public RocketMQConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(RocketMQConfig config) {
        this.config = config;
    }

    @Override
    public DefaultMQProducer getConnection() {
        return producer;
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public DefaultMQPushConsumer createConsumer(String topic, String groupId, String tags) throws MQClientException {
        return RocketMQUtil.createConsumer(config, topic, groupId, tags);
    }

    public void ping() {
        if (producer == null) {
            throw new RocketMQException("RocketMQ Producer 未初始化");
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.shutdown();
            producer = null;
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
