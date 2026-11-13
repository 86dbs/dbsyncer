/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.CaptureTaskRel;

import java.util.List;

/**
 * 远端采集任务与同步任务关联存储。仅在同步任务启动时写入。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
public interface CaptureTaskRelProfile {

    CaptureTaskRel getByTaskId(String taskId);

    List<CaptureTaskRel> listByCaptureTaskId(String captureTaskId);

    /**
     * 写入关联；{@code TASK_ID} 已存在时返回 false。
     *
     * @param rel 关联
     * @return 是否插入成功
     */
    boolean add(CaptureTaskRel rel);

    int removeByTaskId(String taskId);

    int removeByCaptureTaskId(String captureTaskId);
}
