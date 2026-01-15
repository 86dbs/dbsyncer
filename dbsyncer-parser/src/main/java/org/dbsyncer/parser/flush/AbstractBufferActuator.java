/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.flush;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.metric.TimeRegistry;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 任务缓存执行器
 * <p>1. 任务优先进入缓存队列
 * <p>2. 将任务分区合并，批量执行
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:36
 */
public abstract class AbstractBufferActuator<Request extends BufferRequest, Response extends BufferResponse> implements BufferActuator, ScheduledTaskJob {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private Class<Response> responseClazz;
    private Lock taskLock;
    private BufferActuatorConfig config;
    private BlockingQueue<Request> queue;
    private String taskKey;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    protected ProfileComponent profileComponent;

    @Resource
    private TimeRegistry timeRegistry;


    public AbstractBufferActuator() {
        int level = 5;
        Class<?> aClass = getClass();
        while (level > 0) {
            if (aClass.getSuperclass() == AbstractBufferActuator.class) {
                responseClazz = (Class<Response>) ((ParameterizedType) aClass.getGenericSuperclass()).getActualTypeArguments()[1];
                break;
            }
            aClass = aClass.getSuperclass();
            level--;
        }
        Assert.notNull(responseClazz, String.format("%s的父类%s泛型参数Response为空.", getClass().getName(), AbstractBufferActuator.class.getName()));
    }

    /**
     * 初始化配置
     */
    protected void buildConfig() {
        Assert.notNull(config, "请先配置缓存执行器，setConfig(BufferActuatorConfig config)");
        buildQueueConfig();
        // 生成唯一的 taskKey
        if (taskKey == null) {
            taskKey = UUIDUtil.getUUID();
        }
        scheduledTaskService.start(taskKey, config.getBufferPeriodMillisecond(), this);
    }

    /**
     * 初始化缓存队列配置
     */
    protected void buildQueueConfig() {
        taskLock = new ReentrantLock();
        this.queue = new LinkedBlockingQueue<>(config.getBufferQueueCapacity());
    }

    /**
     * 生成分区key
     *
     * @param request
     * @return
     */
    protected abstract String getPartitionKey(Request request);

    /**
     * 分区
     *
     * @param request
     * @param response
     */
    protected abstract void partition(Request request, Response response);

    /**
     * 驱动是否运行中
     *
     * @param request
     * @return
     */
    public boolean isRunning(BufferRequest request) {
        Meta meta = profileComponent.getMeta(request.getMetaId());
        return meta != null && MetaEnum.isRunning(meta.getState());
    }

    /**
     * 是否跳过分区处理
     *
     * @param nextRequest
     * @param response
     * @return
     */
    protected boolean skipPartition(Request nextRequest, Response response) {
        return false;
    }

    /**
     * 批处理
     *
     * @param response
     */
    public abstract void pull(Response response);

    /**
     * 批量处理分区数据
     *
     * @param map
     */
    protected void process(Map<String, Response> map) {
        // 关键修复：按位置顺序处理批次，确保与binlog事件顺序一致
        // 问题：map.forEach() 的顺序不确定（ConcurrentHashMap迭代顺序不确定），
        //      可能导致位置靠后的事件先处理完成，更新快照到更大的位置，跳过位置靠前但还没处理的事件
        // 解决：按位置顺序处理批次，确保不会跳过未处理的事件

        // 为了可维护性，我们将排序后的列表存储起来，以便在处理完所有响应后获取最后一个响应（最大偏移量）
        List<Map.Entry<String, Response>> sortedEntries = map.entrySet().stream()
                .sorted((e1, e2) -> {
                    Response r1 = e1.getValue();
                    Response r2 = e2.getValue();
                    // 检查 Response 是否是 AbstractWriter 的子类（如 WriterResponse）
                    ChangedOffset o1 = null;
                    ChangedOffset o2 = null;
                    if (r1 instanceof org.dbsyncer.parser.model.AbstractWriter) {
                        o1 = ((org.dbsyncer.parser.model.AbstractWriter) r1).getChangedOffset();
                    }
                    if (r2 instanceof org.dbsyncer.parser.model.AbstractWriter) {
                        o2 = ((org.dbsyncer.parser.model.AbstractWriter) r2).getChangedOffset();
                    }

                    // 如果 ChangedOffset 为 null，放在最后处理
                    if (o1 == null && o2 == null) {
                        return 0;
                    }
                    if (o1 == null) {
                        return 1;
                    }
                    if (o2 == null) {
                        return -1;
                    }

                    // 比较文件名
                    String fileName1 = o1.getNextFileName();
                    String fileName2 = o2.getNextFileName();
                    if (fileName1 != null && fileName2 != null) {
                        int fileNameCompare = fileName1.compareTo(fileName2);
                        if (fileNameCompare != 0) {
                            return fileNameCompare;
                        }
                    } else if (fileName1 != null) {
                        return -1;
                    } else if (fileName2 != null) {
                        return 1;
                    }

                    // 比较位置
                    Object pos1 = o1.getPosition();
                    Object pos2 = o2.getPosition();
                    if (pos1 instanceof Long && pos2 instanceof Long) {
                        return Long.compare((Long) pos1, (Long) pos2);
                    } else if (pos1 instanceof Number && pos2 instanceof Number) {
                        return Long.compare(((Number) pos1).longValue(), ((Number) pos2).longValue());
                    } else if (pos1 != null && pos2 != null) {
                        return pos1.toString().compareTo(pos2.toString());
                    } else if (pos1 != null) {
                        return -1;
                    } else if (pos2 != null) {
                        return 1;
                    }

                    return 0;
                })
                .collect(Collectors.toList());

        // 处理所有排序后的响应
        for (Map.Entry<String, Response> entry : sortedEntries) {
            String key = entry.getKey();
            Response response = entry.getValue();
            long now = Instant.now().toEpochMilli();
            try {
                pull(response);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("[{}{}] {}, {}ms", key, response.getSuffixName(), response.getTaskSize(), (Instant.now().toEpochMilli() - now));
        }

        // 获取最后一个响应（如果有）作为最大偏移量响应，传递给后置处理
        WriterResponse lastResponse = null;
        if (!sortedEntries.isEmpty()) {
            lastResponse = (WriterResponse) sortedEntries.get(sortedEntries.size() - 1).getValue();
        }
        
        // 处理完成后，调用钩子方法（子类可以重写来处理偏移量刷新、pending 状态等）
        afterProcess(lastResponse);
    }

    /**
     * 处理完成后的钩子方法（子类可以重写）
     * 
     * @param lastResponse 最后一个响应（可能为 null），包含最大偏移量信息
     */
    protected void afterProcess(WriterResponse lastResponse) {
        // 默认实现为空，子类可以重写
    }


    /**
     * 提交任务失败
     *
     * @param queue
     * @param request
     */
    protected abstract void offerFailed(BlockingQueue<Request> queue, Request request);

    /**
     * 统计消费 TPS
     *
     * @param timeRegistry
     * @param count
     */
    protected void meter(TimeRegistry timeRegistry, long count) {

    }

    @Override
    public void offer(BufferRequest request) {
        if (queue.offer((Request) request)) {
            return;
        }
        if (isRunning(request)) {
            offerFailed(queue, (Request) request);
        }
    }

    @Override
    public void run() {
        boolean locked = false;
        try {
            locked = taskLock.tryLock();
            if (locked) {
                submit();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (locked) {
                taskLock.unlock();
            }
        }
    }

    @Override
    public Queue getQueue() {
        return queue;
    }

    @Override
    public int getQueueCapacity() {
        return config.getBufferQueueCapacity();
    }

    private void submit() {
        if (queue.isEmpty()) {
            return;
        }

        AtomicLong batchCounter = new AtomicLong();
        Map<String, Response> map = new ConcurrentHashMap<>();
        while (!queue.isEmpty()) {
            Request poll = queue.poll();
            String key = getPartitionKey(poll);
            Response response = map.compute(key, (k, v) -> {
                if (v == null) {
                    try {
                        return responseClazz.newInstance();
                    } catch (Exception e) {
                        throw new ParserException(e);
                    }
                }
                return v;
            });
            partition(poll, response);
            batchCounter.incrementAndGet();

            // 当达到批次大小时，处理当前批次
            if (batchCounter.get() >= config.getBufferPullCount()) {
                process(map);
                map.clear();
                meter(timeRegistry, batchCounter.get());
                batchCounter.set(0);
                map = new ConcurrentHashMap<>();
            }

            Request next = queue.peek();
            if (null != next && skipPartition(next, response)) {
                break;
            }
        }

        // 处理剩余未达到批次大小的数据
        if (!map.isEmpty()) {
            process(map);
            map.clear();
            meter(timeRegistry, batchCounter.get());
        }

        map = null;
        batchCounter = null;
    }

    public void setConfig(BufferActuatorConfig config) {
        this.config = config;
    }

    protected int getBufferWriterCount() {
        return config.getBufferWriterCount();
    }

    /**
     * 获取定时任务的 key
     */
    public String getTaskKey() {
        return taskKey;
    }

    /**
     * 设置定时任务的 key（用于手动管理定时任务）
     */
    public void setTaskKey(String taskKey) {
        this.taskKey = taskKey;
    }

    /**
     * 停止执行器（停止定时任务）
     */
    public void stop() {
        if (taskKey != null && scheduledTaskService != null) {
            scheduledTaskService.stop(taskKey);
        }
    }

    /**
     * 设置定时任务服务（用于手动注入依赖）
     */
    public void setScheduledTaskService(ScheduledTaskService scheduledTaskService) {
        this.scheduledTaskService = scheduledTaskService;
    }

    /**
     * 设置 ProfileComponent（用于手动注入依赖）
     */
    public void setProfileComponent(ProfileComponent profileComponent) {
        this.profileComponent = profileComponent;
    }

    /**
     * 设置 TimeRegistry（用于手动注入依赖）
     */
    public void setTimeRegistry(TimeRegistry timeRegistry) {
        this.timeRegistry = timeRegistry;
    }
}