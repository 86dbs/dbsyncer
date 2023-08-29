package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.config.WriteExecutorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.ThreadPoolUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.parser.flush.BufferRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private WriteExecutorConfig writeExecutorConfig;

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
        BufferActuatorConfig actuatorConfig = super.getBufferActuatorConfig();
        try {
            BufferActuatorConfig newConfig = (BufferActuatorConfig) actuatorConfig.clone();
            // TODO 暂定容量上限
            newConfig.setQueueCapacity(50000);
            setBufferActuatorConfig(newConfig);
            running = true;
            super.buildBufferActuatorConfig();
            taskKey = UUIDUtil.getUUID();
            String threadNamePrefix = new StringBuilder("writeExecutor-").append(tableGroupId).append(StringUtil.SYMBOL).toString();
            threadPoolTaskExecutor = ThreadPoolUtil.newThreadPoolTaskExecutor(5, 5, 1000, 30, threadNamePrefix);
            setWriteExecutor(threadPoolTaskExecutor);
            scheduledTaskService.start(taskKey, getBufferActuatorConfig().getPeriodMillisecond(), this);
        } catch (CloneNotSupportedException e) {
            logger.error(e.getMessage(), e);
        }
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