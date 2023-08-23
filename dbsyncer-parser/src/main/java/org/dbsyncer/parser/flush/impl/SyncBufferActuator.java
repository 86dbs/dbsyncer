package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 同步缓冲执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public final class SyncBufferActuator extends WriterBufferActuator implements ScheduledTaskJob {

    @Resource
    private BufferActuatorConfig bufferActuatorConfig;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @PostConstruct
    private void init() {
        super.buildConfig(bufferActuatorConfig);
        scheduledTaskService.start(bufferActuatorConfig.getPeriodMillisecond(), this);
    }

    @Override
    public void run() {
        batchExecute();
    }
}