/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.redis.cdc;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.redis.RedisConnectorInstance;
import org.dbsyncer.connector.redis.RedisException;
import org.dbsyncer.connector.redis.constant.RedisConstant;
import org.dbsyncer.connector.redis.util.RedisUtil;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractListener;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Redis Stream 增量监听器
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:20
 */
public class RedisListener extends AbstractListener<RedisConnectorInstance> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String OFFSET = "pos_";
    private static final String PAYLOAD_FIELD = "payload";

    private final List<StreamConsumer> consumers = new ArrayList<>();
    private final int fetchSize = 500;
    private volatile boolean connected;
    private Worker worker;

    @Override
    public void start() {
        connected = true;
        RedisConnectorInstance instance = getConnectorInstance();
        try {
            for (Table table : customTable) {
                String stream = table.getName();
                String groupId = table.getExtInfo().getProperty(RedisConstant.GROUP_ID);
                if (StringUtil.isBlank(groupId)) {
                    throw new RedisException("Redis Stream 消费组 groupId 不能为空");
                }
                String consumerName = table.getExtInfo().getProperty(RedisConstant.CONSUMER_NAME);
                if (StringUtil.isBlank(consumerName)) {
                    consumerName = RedisUtil.DEFAULT_CONSUMER;
                }
                consumers.add(new StreamConsumer(table, groupId, consumerName));
                ensureGroup(instance, stream, groupId);
                logger.info("Redis监听器已订阅 Stream: {}, groupId: {}, consumer: {}", stream, groupId, consumerName);
            }
            worker = new Worker();
            worker.setName("redis-listener-" + instance.getConfig().getUrl() + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动Redis监听器失败", e);
            errorEvent(e);
            close();
            throw new RedisException("启动Redis监听器失败", e);
        }
    }

    @Override
    public void close() {
        if (!connected) {
            return;
        }
        connected = false;
        if (worker != null && !worker.isInterrupted()) {
            worker.interrupt();
        }
        if (worker != null) {
            try {
                worker.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            worker = null;
        }
        consumers.clear();
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (StringUtil.isNotBlank(offset.getNextFileName()) && offset.getPosition() != null) {
            snapshot.put(OFFSET + offset.getNextFileName(), String.valueOf(offset.getPosition()));
        }
    }

    private void ensureGroup(RedisConnectorInstance instance, String stream, String groupId) {
        Jedis jedis = null;
        try {
            jedis = instance.borrowJedis();
            try {
                jedis.xgroupCreate(stream, groupId, StreamEntryID.LAST_ENTRY, true);
            } catch (Exception e) {
                if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                    throw e;
                }
            }
        } finally {
            RedisUtil.returnResource(instance.getConnection(), jedis);
        }
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
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private Map<String, Object> parsePayload(StreamEntry entry) {
        Map<String, String> fields = entry.getFields();
        if (CollectionUtils.isEmpty(fields)) {
            return new HashMap<>();
        }
        if (fields.containsKey(PAYLOAD_FIELD)) {
            return JsonUtil.jsonToObj(fields.get(PAYLOAD_FIELD), HashMap.class);
        }
        return new HashMap<>(fields);
    }

    private List<Object> mapToRowList(List<Field> columns, Map<String, Object> map) {
        if (map == null || columns == null) {
            return new ArrayList<>();
        }
        return columns.stream().map(c -> map.get(c.getName())).collect(Collectors.toList());
    }

    private void processEntry(StreamConsumer consumerInfo, StreamEntry entry) {
        String stream = consumerInfo.table.getName();
        Map<String, Object> valueMap = parsePayload(entry);
        List<Object> rowData = mapToRowList(consumerInfo.table.getColumn(), valueMap);
        String offsetId = entry.getID().toString();
        trySendEvent(new RowChangedEvent(stream, ConnectorConstant.OPERTION_INSERT, rowData, stream, offsetId));
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            RedisConnectorInstance instance = getConnectorInstance();
            while (!isInterrupted() && connected) {
                try {
                    for (StreamConsumer consumerInfo : consumers) {
                        pollStream(instance, consumerInfo);
                    }
                    if (connected && !isInterrupted()) {
                        sleepInMills(10);
                    }
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    logger.error(e.getMessage(), e);
                    errorEvent(e);
                    sleepInMills(1000);
                }
            }
            logger.info("Redis监听器Worker线程已退出");
        }

        private void pollStream(RedisConnectorInstance instance, StreamConsumer consumerInfo) {
            Jedis jedis = null;
            try {
                jedis = instance.borrowJedis();
                StreamEntryID startId = resolveStartId(consumerInfo.table.getName());
                XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(fetchSize).block(200);
                Map<String, StreamEntryID> streams = new HashMap<>();
                streams.put(consumerInfo.table.getName(), startId);
                List<Map.Entry<String, List<StreamEntry>>> result = jedis.xreadGroup(
                        consumerInfo.groupId, consumerInfo.consumerName, params, streams);
                if (CollectionUtils.isEmpty(result)) {
                    return;
                }
                for (Map.Entry<String, List<StreamEntry>> item : result) {
                    List<StreamEntry> entries = item.getValue();
                    if (CollectionUtils.isEmpty(entries)) {
                        continue;
                    }
                    for (StreamEntry entry : entries) {
                        processEntry(consumerInfo, entry);
                    }
                    jedis.xack(consumerInfo.table.getName(), consumerInfo.groupId,
                            entries.get(entries.size() - 1).getID());
                }
            } catch (Exception e) {
                if (!Thread.currentThread().isInterrupted()) {
                    logger.error("读取 Stream {} 失败", consumerInfo.table.getName(), e);
                    errorEvent(e);
                }
            } finally {
                RedisUtil.returnResource(instance.getConnection(), jedis);
            }
        }

        private StreamEntryID resolveStartId(String stream) {
            String offsetStr = snapshot.get(OFFSET + stream);
            if (StringUtil.isNotBlank(offsetStr)) {
                return new StreamEntryID(offsetStr);
            }
            return StreamEntryID.UNRECEIVED_ENTRY;
        }
    }

    static final class StreamConsumer {
        final Table table;
        final String groupId;
        final String consumerName;

        StreamConsumer(Table table, String groupId, String consumerName) {
            this.table = table;
            this.groupId = groupId;
            this.consumerName = consumerName;
        }
    }
}
