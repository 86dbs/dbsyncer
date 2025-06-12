/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.scheduler.mapping;

import org.dbsyncer.biz.enums.TaskSchedulerEnum;
import org.dbsyncer.biz.scheduler.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计算驱动总数任务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-13 00:00
 */
public class MappingCountTask implements Task {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String mappingId;

    public MappingCountTask(String mappingId) {
        this.mappingId = mappingId;
    }

    @Override
    public TaskSchedulerEnum getType() {
        return TaskSchedulerEnum.MAPPING_COUNT;
    }

    @Override
    public void run() {
        logger.info("正在统计驱动总数 ({})", mappingId);
    }
}