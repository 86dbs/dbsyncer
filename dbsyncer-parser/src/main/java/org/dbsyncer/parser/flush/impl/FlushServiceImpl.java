package org.dbsyncer.parser.flush.impl;

import com.alibaba.fastjson.JSONException;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.parser.flush.FlushService;
import org.dbsyncer.storage.SnowflakeIdWorker;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.enums.StorageEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 持久化
 * <p>全量或增量数据</p>
 * <p>系统日志</p>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/05/19 18:38
 */
@Component
public class FlushServiceImpl implements FlushService, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    @Autowired
    private SnowflakeIdWorker snowflakeIdWorker;

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    @Autowired
    private Executor taskExecutor;

    private Queue<Task> buffer = new ConcurrentLinkedQueue();

    private Queue<Task> temp = new ConcurrentLinkedQueue();

    private final Object LOCK = new Object();

    private volatile boolean running;

    @PostConstruct
    private void init() {
        scheduledTaskService.start("*/3 * * * * ?", this);
    }

    @Override
    public void asyncWrite(String type, String error) {
        Map<String, Object> params = new HashMap();
        params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, type);
        params.put(ConfigConstant.CONFIG_MODEL_JSON, error);
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, Instant.now().toEpochMilli());
        storageService.addLog(StorageEnum.LOG, params);
    }

    @Override
    public void asyncWrite(String metaId, String event, boolean success, List<Map> data, String error) {
        long now = Instant.now().toEpochMilli();
        AtomicBoolean added = new AtomicBoolean(false);
        List<Map> list = data.parallelStream().map(r -> {
            Map<String, Object> params = new HashMap();
            params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
            params.put(ConfigConstant.DATA_SUCCESS, success ? StorageDataStatusEnum.SUCCESS.getValue() : StorageDataStatusEnum.FAIL.getValue());
            params.put(ConfigConstant.DATA_EVENT, event);
            params.put(ConfigConstant.DATA_ERROR, added.get() ? "" : error);
            try {
                params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(r));
            } catch (JSONException e) {
                logger.warn("可能存在Blob或inputStream大文件类型, 无法序列化:{}", r);
                params.put(ConfigConstant.CONFIG_MODEL_JSON, r.toString());
            }
            params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            added.set(true);
            return params;
        }).collect(Collectors.toList());

        if (running) {
            temp.offer(new Task(metaId, list));
            return;
        }

        buffer.offer(new Task(metaId, list));
    }

    @Override
    public void run() {
        if (running) {
            return;
        }
        synchronized (LOCK) {
            if (running) {
                return;
            }
            running = true;
            flush(buffer);
            running = false;
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
            flush(temp);
        }
    }

    private void flush(Queue<Task> buffer) {
        if (!buffer.isEmpty()) {
            final Map<String, List<Map>> task = new LinkedHashMap<>();
            while (!buffer.isEmpty()) {
                Task t = buffer.poll();
                if (!task.containsKey(t.metaId)) {
                    task.putIfAbsent(t.metaId, new LinkedList<>());
                }
                task.get(t.metaId).addAll(t.list);
            }
            task.forEach((metaId, list) -> {
                taskExecutor.execute(() -> {
                    long now = Instant.now().toEpochMilli();
                    try {
                        storageService.addData(StorageEnum.DATA, metaId, list);
                    } catch (Exception e) {
                        logger.error("[{}]-flush异常{}", metaId, list.size());
                    }
                    logger.info("[{}]-flush{}条，耗时{}秒", metaId, list.size(), (Instant.now().toEpochMilli() - now) / 1000);
                });
            });
            task.clear();
        }
    }

    final class Task {
        String metaId;
        List<Map> list;

        public Task(String metaId, List<Map> list) {
            this.metaId = metaId;
            this.list = list;
        }
    }

}