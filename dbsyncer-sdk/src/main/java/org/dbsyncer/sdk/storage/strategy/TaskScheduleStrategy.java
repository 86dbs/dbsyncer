/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 任务调度：单表存储 {@code dbsyncer_task_schedule}。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-01
 */
public final class TaskScheduleStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.TASK_SCHEDULE.getType();
    }
}
