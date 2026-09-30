/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.Meta;

/**
 * 任务明细meta执行结果表
 *
 * @author wuji
 * @version 1.0.0
 */
public interface TaskDetailMetaProfile {

    /**
     * 获取任务的meta缓存
     */
    Meta getMeta(String taskId);

    /**
     * 添加 Meta。
     */
    String add(Meta meta);

    /**
     * 更新 Meta。
     */
    String update(Meta meta);

    /**
     * 删除 Meta。
     */
    void remove(String taskId);

    /**
     * 重置meta状态
     */
    void reset(String taskId);

    /**
     * 清空表, 删除task_detail_{任务id}数据
     */
    void clearData(String id);
}
