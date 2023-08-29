package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.TableGroupBufferConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.ThreadPoolUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.parser.flush.BufferRequest;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 表执行器（根据表消费数据，多线程批量写，按序执行）
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public final class TableGroupBufferActuator extends GeneralBufferActuator implements Cloneable {

    @Resource
    private TableGroupBufferConfig tableGroupBufferConfig;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    private String taskKey;

    private String tableGroupId;

    private volatile boolean running;

    @Override
    public void init() {
        // nothing to do
    }

    @Override
    protected boolean isRunning(BufferRequest request) {
        return running;
    }

    public void buildConfig() {
        super.setConfig(tableGroupBufferConfig);
        super.buildQueueConfig();
        taskKey = UUIDUtil.getUUID();
        int coreSize = tableGroupBufferConfig.getThreadCoreSize();
        int queueCapacity = tableGroupBufferConfig.getThreadQueueCapacity();
        String threadNamePrefix = new StringBuilder("TableGroupExecutor-").append(tableGroupId).append(StringUtil.SYMBOL).toString();
        threadPoolTaskExecutor = ThreadPoolUtil.newThreadPoolTaskExecutor(coreSize, coreSize, queueCapacity, 30, threadNamePrefix);
        setGeneralExecutor(threadPoolTaskExecutor);
        running = true;
        scheduledTaskService.start(taskKey, tableGroupBufferConfig.getBufferPeriodMillisecond(), this);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public void stop() {
        running = false;
        if (threadPoolTaskExecutor != null) {
            threadPoolTaskExecutor.shutdown();
        }
        scheduledTaskService.stop(taskKey);
    }

    public String getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(String tableGroupId) {
        this.tableGroupId = tableGroupId;
    }
}