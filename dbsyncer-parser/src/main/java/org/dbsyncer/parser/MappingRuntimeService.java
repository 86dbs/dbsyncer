/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.parser.model.Mapping;

/**
 * Mapping 运行时控制：启停本机 Puller、更新任务状态。供跨模块调用。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-26
 */
public interface MappingRuntimeService {

    /**
     * 启动驱动。
     *
     * @param mapping      驱动
     * @param autoRecovery 是否自动恢复
     */
    void start(Mapping mapping, boolean autoRecovery);

    /**
     * 仅本机拉起 Puller，不派工、不改 Meta、失败不清调度。
     *
     * @param mapping      驱动
     * @param autoRecovery 是否自动恢复
     */
    void startLocal(Mapping mapping, boolean autoRecovery);

    /**
     * 仅停止本进程 Puller，不改集群任务状态。
     *
     * @param mapping 驱动
     */
    void stopLocal(Mapping mapping);

    /**
     * 本进程是否已启动该驱动。
     *
     * @param metaId Meta ID
     * @return true 已启动
     */
    boolean isLocalActive(String metaId);

    /**
     * 更新任务 Meta 状态。
     *
     * @param metaId Meta ID
     * @param status 目标状态
     */
    void changeMetaState(String metaId, CommonTaskStatusEnum status);
}
