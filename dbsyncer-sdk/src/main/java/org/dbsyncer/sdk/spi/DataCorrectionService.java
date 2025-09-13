/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.sdk.model.CommonTask;

import java.util.List;
import java.util.Map;

/**
 * 数据订正服务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-05-12 23:37
 */
public interface DataCorrectionService {

    /**
     * 新增任务
     *
     * @param params
     */
    boolean add(Map<String, String> params);

    /**
     * 任务状态
     *
     * @param taskId
     * @return
     */
    boolean start(String taskId);

    /**
     * 停止任务
     *
     * @param taskId
     * @return
     */
    boolean stop(String taskId);

    /**
     * 重启任务
     *
     * @param taskId
     * @return
     */
    boolean restart(String taskId);

    /**
     * 暂停任务
     *
     * @param taskId
     * @return
     */
    boolean pause(String taskId);

    /**
     * 继续任务
     *
     * @param taskId
     * @return
     */
    boolean resume(String taskId);

    /**
     * 任务详情
     *
     * @param taskId
     * @return
     */
    CommonTask detail(String taskId);

    /**
     * 任务列表
     *
     * @param type
     * @return
     */
    List<CommonTask> list(String type);

}