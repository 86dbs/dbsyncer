/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.scheduler;

import org.dbsyncer.biz.enums.TaskSchedulerEnum;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-12 23:54
 */
public interface Task extends Runnable {

    TaskSchedulerEnum getType();

}