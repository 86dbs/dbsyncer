/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.cdc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.BatchTaskUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.kafka.KafkaConnectorInstance;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.connector.kafka.util.KafkaUtil;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kafka监听器，支持批量拉取多个topic数据
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-10 14:58
 */
public class KafkaListener extends AbstractListener<KafkaConnectorInstance> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<ConsumerInfo> consumers = new ArrayList<>();

    private final int fetchSize = 1000;
    private final int consumerThreadSize = 5;
    private volatile boolean connected = false;
    private Worker worker;
    private ExecutorService executor;

    @Override
    public void start() {
        connected = true;
        KafkaConnectorInstance instance = getConnectorInstance();
        KafkaConfig config = instance.getConfig();

        try {
            int coreSize = Runtime.getRuntime().availableProcessors();
            executor = BatchTaskUtil.createExecutor(coreSize, coreSize * 2, 128);
            for (Table table : customTable) {
                String topic = table.getName();
                String groupId = table.getExtInfo().getProperty("groupId");

                Properties properties = KafkaUtil.parse(config.getProperties().getProperty("consumerProperties"));
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getUrl());
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                props.putAll(properties);

                KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));
                // 恢复offset
                restoreOffset(consumer, topic);
                consumers.add(new ConsumerInfo(table, consumer));
                logger.info("Kafka监听器已订阅topic: {}, groupId: {}", topic, groupId);
            }

            worker = new Worker();
            worker.setName("kafka-listener-" + config.getUrl() + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动Kafka监听器失败", e);
            errorEvent(e);
            throw new RuntimeException("启动Kafka监听器失败", e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (connected) {
            connected = false;
            if (null != worker && !worker.isInterrupted()) {
                worker.interrupt();
                worker = null;
            }
            // 关闭所有consumer
            consumers.forEach(ConsumerInfo::close);
            consumers.clear();
            executor.shutdown();
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (StringUtil.isNotBlank(offset.getNextFileName()) && offset.getPosition() != null) {
            snapshot.put(offset.getNextFileName(), String.valueOf(offset.getPosition()));
        }
    }

    /**
     * 恢复offset
     */
    private void restoreOffset(KafkaConsumer<String, Object> consumer, String topic) {
        String offsetStr = snapshot.get(topic);
        if (StringUtil.isNotBlank(offsetStr)) {
            long offset = Long.parseLong(offsetStr);
            // 获取该topic的所有分区
            Set<TopicPartition> partitions = consumer.assignment();
            if (!partitions.isEmpty()) {
                // 为所有属于该topic的分区设置offset
                for (TopicPartition partition : partitions) {
                    if (partition.topic().equals(topic)) {
                        consumer.seek(partition, offset);
                        logger.info("已恢复topic {} 分区 {} 的offset: {}", topic, partition.partition(), offset);
                    }
                }
            }
        }
    }

    /**
     * 将Map转换为List<Object>，按照字段顺序
     */
    private List<Object> mapToRowList(List<Field> columns, Map<String, Object> map) {
        if (map == null || columns == null) {
            return new ArrayList<>();
        }
        return columns.stream().map(c -> map.get(c.getName())).collect(Collectors.toList());
    }

    private void trySendEvent(ChangedEvent event) {
        try {
            // 如果消费事件失败，重试
            while (connected) {
                try {
                    changeEvent(event);
                    break;
                } catch (QueueOverflowException e) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 处理单条消息
     */
    private void processRecord(ConsumerInfo consumerInfo, ConsumerRecord<String, Object> record) {
        String topic = consumerInfo.table.getName();
        Object value = record.value();
        if (value == null) {
            logger.warn("收到空消息，topic: {}, offset: {}", topic, record.offset());
            return;
        }

        // 将value转换为Map
        Map<String, Object> valueMap;
        if (value instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> tempMap = (Map<String, Object>) value;
            valueMap = tempMap;
        } else {
            logger.warn("消息value类型不是Map，topic: {}, offset: {}, type: {}", topic, record.offset(), value.getClass().getName());
            return;
        }

        // 转换为行数据
        List<Object> rowData = mapToRowList(consumerInfo.table.getColumn(), valueMap);

        // 触发事件
        trySendEvent(new RowChangedEvent(topic, ConnectorConstant.OPERTION_INSERT, rowData, topic, record.offset()));
    }

    final class Worker extends Thread {

        int total = consumers.size();

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    int offset = 0;
                    do {
                        List<ConsumerInfo> collect = consumers.stream().skip(offset).limit(consumerThreadSize).collect(Collectors.toList());
                        BatchTaskUtil.execute(executor, collect, this::execute, logger);
                        offset = offset + consumerThreadSize;
                    } while (offset < total);

                    if (connected && !isInterrupted()) {
                        sleepInMills(10);
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        logger.info("Kafka监听器Worker线程被中断");
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Kafka监听器Worker线程发生异常", e);
                    errorEvent(e);
                    sleepInMills(1000);
                }
            }
            logger.info("Kafka监听器Worker线程已退出");
        }

        private void execute(ConsumerInfo consumerInfo) {
            try {
                long batch = 0;
                while (connected && batch < fetchSize) {
                    ConsumerRecords<String, Object> records = consumerInfo.consumer.poll(100);
                    if (records.isEmpty()) {
                        break;
                    }
                    for (ConsumerRecord<String, Object> record : records) {
                        processRecord(consumerInfo, record);
                        batch++;
                    }
                    // 手动提交offset（如果启用了手动提交）
                    consumerInfo.consumer.commitSync();
                }
            } catch (Exception e) {
                logger.error("处理topic {} 的消息时出错", consumerInfo.table.getName(), e);
                errorEvent(e);
            }
        }

    }

    static final class ConsumerInfo {
        Table table;
        KafkaConsumer<String, Object> consumer;

        public ConsumerInfo(Table table, KafkaConsumer<String, Object> consumer) {
            this.table = table;
            this.consumer = consumer;
        }

        void close() {
            consumer.close();
        }
    }
}
