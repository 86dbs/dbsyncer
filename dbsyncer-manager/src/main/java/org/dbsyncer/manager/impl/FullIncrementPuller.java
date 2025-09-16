/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.Puller;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.enums.SyncPhaseEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 全量+增量混合同步
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-05-01
 */
@Component
public final class FullIncrementPuller implements Puller {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private FullPuller fullPuller;

    @Resource
    private IncrementPuller incrementPuller;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Resource
    private ConnectorFactory connectorFactory;

    // 简化的受保护字段名常量
    private static final String PROTECTED_INCREMENT_INFO = "_protected_increment_info";

    @Override
    public void start(Mapping mapping) {
        final String metaId = mapping.getMetaId();

        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        Thread coordinator = new Thread(() -> {
            try {
                // 1. 检查故障恢复（零开销）
                // 修改为从 Meta 对象直接获取 syncPhase
                SyncPhaseEnum recoveryPhase = meta.getSyncPhase();

                // 2. 根据阶段直接执行（内联逻辑，简化设计）
                switch (recoveryPhase) {
                    case FULL:
                        // 记录增量起始点并执行全量同步
                        recordIncrementStartPoint(mapping, meta);
                        startFullThenIncrement(mapping, meta);
                        break;

                    case INCREMENTAL:
                        // 直接启动增量（全量已完成）
                        startIncrementSync(mapping, meta);
                        break;

                    default:
                        throw new ManagerException("不支持的恢复阶段: " + recoveryPhase);
                }

            } catch (Exception e) {
                // 异常驱动：直接更新Meta状态为ERROR并记录错误信息
                meta.saveState(MetaEnum.ERROR, e.getMessage());
                logger.error("混合同步异常，已更新Meta状态为ERROR: {}", metaId, e);
                // 在logService中记录异常信息
                logService.log(LogType.TableGroupLog.FULL_FAILED, "混合同步异常: %s，错误信息: %s", metaId, e.getMessage());
            }
        });

        coordinator.setName("full-increment-coordinator-" + mapping.getId());
        coordinator.start();
    }

    @Override
    public void close(Meta meta) {
        // 关闭全量和增量同步
        fullPuller.close(meta);
        incrementPuller.close(meta);
    }

    // 核心：运行全量同步并在完成后执行回调
    private void startFullThenIncrement(Mapping mapping, Meta meta) {
        // 设置阶段回调：全量完成后启动增量同步
        meta.setPhaseHandler(() -> {
            // 启动增量同步
            startIncrementSync(mapping, meta);
        });

        // 启动全量同步
        fullPuller.start(mapping);
    }

    /**
     * 记录增量起始点
     *
     * @param mapping
     */
    private void recordIncrementStartPoint(Mapping mapping, Meta meta) {
        // 检查是否已经记录了增量起始点
        if (meta.isIncrementStartPointRecorded()) {
            logger.info("增量起始点已记录，跳过: {}", mapping.getMetaId());
            return;
        }

        // 简化设计：委托给连接器获取当前位置
        ConnectorConfig sourceConfig = profileComponent.getConnector(mapping.getSourceConnectorId()).getConfig();
        ConnectorService connectorService = connectorFactory.getConnectorService(sourceConfig.getConnectorType());
        ConnectorInstance connectorInstance = connectorFactory.connect(sourceConfig);

        try {
            // 使用现有的getPosition方法，返回当前位置
            Map<String, String> currentPosition = connectorService.getPosition(connectorInstance);
            if (currentPosition == null || currentPosition.isEmpty()) {
                throw new RuntimeException("无法获取当前位置信息！");
            }
            meta.recordIncrementStartPoint(currentPosition);
        } finally {
            // 清理连接资源
            connectorService.disconnect(connectorInstance);
        }
        logger.info("已记录增量同步起始位置: metaId={}", mapping.getMetaId());
    }

    private void startIncrementSync(Mapping mapping, Meta meta) {
        // 关键：恢复受保护的增量起始点到正常字段
        meta.restoreProtectedIncrementStartPoint();

        // 直接使用原始Mapping启动增量同步
        incrementPuller.start(mapping);

        logger.info("增量同步已启动，混合模式进入持续运行状态: {}", mapping.getMetaId());
    }

    /**
     * 重置混合同步任务
     * 
     * @param meta 混合同步任务元数据
     */
    public void reset(Meta meta) {
        meta.setSyncPhase(SyncPhaseEnum.FULL);
        // 关闭当前正在运行的任务
        fullPuller.reset(meta);
        incrementPuller.reset(meta);
        logger.info("混合同步任务已重置: {}", meta.getId());
    }
}