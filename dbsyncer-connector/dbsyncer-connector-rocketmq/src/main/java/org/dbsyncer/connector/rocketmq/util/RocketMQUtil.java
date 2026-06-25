/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.rocketmq.util;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.rocketmq.config.RocketMQConfig;
import org.dbsyncer.sdk.util.PropertiesUtil;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

import java.util.Properties;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 01:00
 */
public abstract class RocketMQUtil {

    public static final String PRODUCER_PROPERTIES = "producerProperties";

    public static final String CONSUMER_PROPERTIES = "consumerProperties";

    public static final String DEFAULT_PRODUCER_GROUP = "dbsyncer-producer";

    public static DefaultMQProducer createProducer(RocketMQConfig config) throws MQClientException {
        Properties props = parse(config.getExtInfo().getProperty(PRODUCER_PROPERTIES));
        String producerGroup = props.getProperty("producerGroup", DEFAULT_PRODUCER_GROUP);
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(config.getUrl());
        producer.setVipChannelEnabled(false);
        applyProducerProperties(producer, props);
        producer.start();
        return producer;
    }

    public static DefaultMQPushConsumer createConsumer(RocketMQConfig config, String topic, String groupId, String tags) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupId);
        consumer.setNamesrvAddr(config.getUrl());
        consumer.setVipChannelEnabled(false);
        consumer.subscribe(topic, StringUtil.isBlank(tags) ? "*" : tags);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        Properties props = parse(config.getExtInfo().getProperty(CONSUMER_PROPERTIES));
        applyConsumerProperties(consumer, props);
        return consumer;
    }

    public static Properties parse(String properties) {
        if (StringUtil.isBlank(properties)) {
            return new Properties();
        }
        String normalized = properties.replaceAll("\r\n", "&").replaceAll("\n", "&");
        return PropertiesUtil.parse(normalized);
    }

    public static String toString(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        return PropertiesUtil.toString(properties).replaceAll("&", "\r\n");
    }

    private static void applyProducerProperties(DefaultMQProducer producer, Properties props) {
        setIntProperty(props, "sendMsgTimeout", producer::setSendMsgTimeout);
        setIntProperty(props, "retryTimesWhenSendFailed", producer::setRetryTimesWhenSendFailed);
        setIntProperty(props, "retryTimesWhenSendAsyncFailed", producer::setRetryTimesWhenSendAsyncFailed);
        setIntProperty(props, "compressMsgBodyOverHowmuch", producer::setCompressMsgBodyOverHowmuch);
        setIntProperty(props, "maxMessageSize", producer::setMaxMessageSize);
    }

    private static void applyConsumerProperties(DefaultMQPushConsumer consumer, Properties props) {
        setIntProperty(props, "consumeThreadMin", consumer::setConsumeThreadMin);
        setIntProperty(props, "consumeThreadMax", consumer::setConsumeThreadMax);
        setIntProperty(props, "consumeMessageBatchMaxSize", consumer::setConsumeMessageBatchMaxSize);
        setIntProperty(props, "pullBatchSize", consumer::setPullBatchSize);
        setIntProperty(props, "maxReconsumeTimes", consumer::setMaxReconsumeTimes);
    }

    private static void setIntProperty(Properties props, String key, IntConsumer consumer) {
        if (props.containsKey(key)) {
            consumer.accept(Integer.parseInt(props.getProperty(key)));
        }
    }

    @FunctionalInterface
    private interface IntConsumer {
        void accept(int value);
    }
}
