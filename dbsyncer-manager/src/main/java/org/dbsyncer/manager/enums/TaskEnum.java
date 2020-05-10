package org.dbsyncer.manager.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.model.Task;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.extractor.increment.LogIncrement;
import org.dbsyncer.manager.extractor.increment.TimingIncrement;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum TaskEnum {

    /**
     * 定时
     */
    TIMING("timing", new TimingIncrement()),
    /**
     * 监听日志
     */
    LOG("log", new LogIncrement());

    private String type;
    private Task task;

    TaskEnum(String type, Task task) {
        this.type = type;
        this.task = task;
    }

    /**
     * 获取同步任务类型
     *
     * @param type
     * @return
     * @throws ManagerException
     */
    public static Task getIncrementTask(String type) throws ManagerException {
        for (TaskEnum e : TaskEnum.values()) {
            if (StringUtils.equals(type, e.getType())) {
                return e.getTask();
            }
        }
        throw new ManagerException(String.format("Task type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Task getTask() {
        return task;
    }
}
