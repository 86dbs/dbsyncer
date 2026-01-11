/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka.cdc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;
import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.BatchTaskUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.kafka.KafkaConnectorInstance;
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
import java.util.List;
import java.util.Map;
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

    private static final String OFFSET = "pos_";
    private final int fetchSize = 1000;
    private final int consumerThreadSize = 5;
    private volatile boolean connected = false;
    private Worker worker;
    private ExecutorService executor;

    @Override
    public void start() {
        connected = true;
        KafkaConnectorInstance instance = getConnectorInstance();
        try {
            int coreSize = Runtime.getRuntime().availableProcessors();
            int queueSize = Math.min(customTable.size(), 128);
            executor = BatchTaskUtil.createExecutor(coreSize, coreSize * 2, queueSize);
            for (Table table : customTable) {
                String topic = table.getName();
                String groupId = table.getExtInfo().getProperty("groupId");
                KafkaConsumer<String, Object> consumer = instance.getConsumer(topic, groupId);
                consumers.add(new ConsumerInfo(table, consumer));
                logger.info("Kafka监听器已订阅topic: {}, groupId: {}", topic, groupId);
            }

            worker = new Worker();
            worker.setName("kafka-listener-" + instance.getConfig().getUrl() + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动Kafka监听器失败", e);
            errorEvent(e);
            close();
            throw new RuntimeException("启动Kafka监听器失败", e);
        }
    }

    @Override
    public void close() {
        if (connected) {
            connected = false;
            // 1. 先中断 Worker 线程
            if (null != worker && !worker.isInterrupted()) {
                worker.interrupt();
            }
            // 2. 等待 Worker 线程退出（最多等待5秒）
            if (null != worker) {
                try {
                    worker.join(5000); // 等待最多5秒
                    if (worker.isAlive()) {
                        logger.warn("Worker线程未在5秒内退出，强制终止");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("等待Worker线程退出时被中断", e);
                }
                worker = null;
            }
            
            // 3. 关闭线程池，等待任务完成
            if (executor != null) {
                executor.shutdown();
                try {
                    // 等待已提交的任务完成，最多等待5秒
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.warn("线程池未在5秒内关闭，强制关闭");
                        executor.shutdownNow();
                        // 再等待2秒
                        if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                            logger.error("线程池无法正常关闭");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("等待线程池关闭时被中断", e);
                    executor.shutdownNow();
                }
            }
            
            // 4. 最后关闭所有consumer（此时没有线程在使用它们）
            consumers.forEach(ConsumerInfo::close);
            consumers.clear();
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (StringUtil.isNotBlank(offset.getNextFileName()) && offset.getPosition() != null) {
            snapshot.put(OFFSET + offset.getNextFileName(), String.valueOf(offset.getPosition()));
        }
    }

    /**
     * 恢复offset，在分区分配后进行
     * 注意：Kafka Consumer 的分区分配是异步的，需要在第一次 poll 后才能获取到分区
     */
    private void seekOffset(KafkaConsumer<String, Object> consumer, String topic) {
        String offsetStr = snapshot.get(OFFSET + topic);
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
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        logger.info("Kafka监听器Worker线程被中断，正在退出");
                        break;
                    }
                    // 非中断异常才记录错误
                    logger.error(e.getMessage(), e);
                    errorEvent(e);
                    if (connected && !isInterrupted()) {
                        sleepInMills(1000);
                    }
                }
            }
            logger.info("Kafka监听器Worker线程已退出");
        }

        private void execute(ConsumerInfo consumerInfo) {
            try {
                // 第一次 poll 之前，先进行一次 poll 来触发分区分配
                // 然后在分区分配之后恢复 offset
                if (!consumerInfo.offsetRestored) {
                    // 使用较短的超时时间来触发分区分配
                    consumerInfo.consumer.poll(100);
                    Set<TopicPartition> partitions = consumerInfo.consumer.assignment();
                    logger.info("Consumer分区分配完成，topic: {}, groupId: {}, 分区数量: {}", 
                        consumerInfo.table.getName(), consumerInfo.table.getExtInfo().getProperty("groupId"), partitions.size());
                    if (!partitions.isEmpty()) {
                        for (TopicPartition partition : partitions) {
                            long position = consumerInfo.consumer.position(partition);
                            logger.info("已分配分区: {}, 当前offset: {}", partition, position);
                        }
                    } else {
                        logger.warn("Consumer分区分配为空，topic: {}, 可能topic不存在或consumer group有问题", consumerInfo.table.getName());
                    }
                    // 现在分区已经分配，可以恢复 offset
                    seekOffset(consumerInfo.consumer, consumerInfo.table.getName());
                    consumerInfo.offsetRestored = true;
                    logger.info("Offset恢复完成，topic: {}", consumerInfo.table.getName());
                }
                
                long processedCount = 0;
                while (connected && processedCount < fetchSize) {
                    ConsumerRecords<String, Object> records = consumerInfo.consumer.poll(100);
                    if (records.isEmpty()) {
                        break;
                    }
                    for (ConsumerRecord<String, Object> record : records) {
                        processRecord(consumerInfo, record);
                        processedCount++;
                    }
                    // 手动提交offset（如果启用了手动提交）
                    if (processedCount > 0) {
                        consumerInfo.consumer.commitSync();
                    }
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    logger.info("Consumer线程已停止");
                    return;
                }
                // 非中断异常才记录错误
                logger.error("处理topic {} 的消息时出错", consumerInfo.table.getName(), e);
                errorEvent(e);
            }
        }

    }

    static final class ConsumerInfo {
        Table table;
        KafkaConsumer<String, Object> consumer;
        private final Object closeLock = new Object(); // 用于同步关闭操作
        private volatile boolean offsetRestored = false; // 标记是否已恢复offset

        public ConsumerInfo(Table table, KafkaConsumer<String, Object> consumer) {
            this.table = table;
            this.consumer = consumer;
        }

        void close() {
            // 使用同步锁确保线程安全关闭
            synchronized (closeLock) {
                if (consumer != null) {
                    try {
                        // 先唤醒consumer（如果它在poll中阻塞）
                        consumer.wakeup();
                        // 然后关闭
                        consumer.close();
                    } catch (Exception e) {
                        // 忽略关闭时的异常
                    }
                }
            }
        }
    }
}
