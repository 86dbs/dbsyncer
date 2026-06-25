/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.rocketmq.cdc;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.rocketmq.RocketMQConnectorInstance;
import org.dbsyncer.connector.rocketmq.util.RocketMQMessageUtil;
import org.dbsyncer.sdk.listener.AbstractListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * RocketMQ 监听器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 01:00
 */
public class RocketMQListener extends AbstractListener<RocketMQConnectorInstance> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String OFFSET = "pos_";

    private final List<ConsumerInfo> consumers = new ArrayList<>();

    private volatile boolean connected = false;

    @Override
    public void start() {
        connected = true;
        RocketMQConnectorInstance instance = getConnectorInstance();
        try {
            for (Table table : customTable) {
                String topic = table.getName();
                String groupId = table.getExtInfo().getProperty("groupId");
                String tags = table.getExtInfo().getProperty("tags");
                if (StringUtil.isBlank(groupId)) {
                    throw new IllegalArgumentException("RocketMQ 消费组 groupId 不能为空, topic: " + topic);
                }
                DefaultMQPushConsumer consumer = instance.createConsumer(topic, groupId, tags);
                consumer.registerMessageListener(createMessageListener(table));
                consumer.start();
                consumers.add(new ConsumerInfo(table, consumer));
                logger.info("RocketMQ监听器已订阅 topic: {}, groupId: {}, tags: {}", topic, groupId, StringUtil.isBlank(tags) ? "*" : tags);
            }
        } catch (Exception e) {
            logger.error("启动RocketMQ监听器失败", e);
            errorEvent(e);
            close();
            throw new RuntimeException("启动RocketMQ监听器失败", e);
        }
    }

    @Override
    public void close() {
        if (!connected) {
            return;
        }
        connected = false;
        consumers.forEach(ConsumerInfo::close);
        consumers.clear();
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (StringUtil.isNotBlank(offset.getNextFileName()) && offset.getPosition() != null) {
            snapshot.put(OFFSET + offset.getNextFileName(), String.valueOf(offset.getPosition()));
        }
    }

    private MessageListenerConcurrently createMessageListener(Table table) {
        return (msgs, context) -> {
            for (MessageExt message : msgs) {
                if (!connected) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                processMessage(table, message);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        };
    }

    private void processMessage(Table table, MessageExt message) {
        String topic = table.getName();
        byte[] body = message.getBody();
        if (body == null || body.length == 0) {
            logger.warn("收到空消息，topic: {}, msgId: {}", topic, message.getMsgId());
            return;
        }

        Map<String, Object> valueMap = JsonUtil.jsonToObj(new String(body, StandardCharsets.UTF_8), HashMap.class);
        if (valueMap == null) {
            logger.warn("消息体解析失败，topic: {}, msgId: {}", topic, message.getMsgId());
            return;
        }

        RocketMQMessageUtil.ParsedMessage parsedMessage = RocketMQMessageUtil.parse(valueMap);
        Map<String, Object> rowMap = parsedMessage.getData();
        String sourceTableName = StringUtil.isNotBlank(parsedMessage.getTable()) ? parsedMessage.getTable() : topic;
        String event = parsedMessage.getEvent();
        List<Object> rowData = mapToRowList(table.getColumn(), rowMap);
        trySendEvent(new RowChangedEvent(sourceTableName, event, rowData, topic, message.getQueueOffset() + 1));
    }

    private List<Object> mapToRowList(List<Field> columns, Map<String, Object> map) {
        if (map == null || columns == null) {
            return new ArrayList<>();
        }
        return columns.stream().map(c -> map.get(c.getName())).collect(Collectors.toList());
    }

    private void trySendEvent(ChangedEvent event) {
        try {
            while (connected) {
                try {
                    changeEvent(event);
                    break;
                } catch (QueueOverflowException e) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        logger.error(ex.getMessage(), ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    static final class ConsumerInfo {

        private final Table table;
        private final DefaultMQPushConsumer consumer;

        ConsumerInfo(Table table, DefaultMQPushConsumer consumer) {
            this.table = table;
            this.consumer = consumer;
        }

        void close() {
            if (consumer != null) {
                consumer.shutdown();
            }
        }
    }
}
