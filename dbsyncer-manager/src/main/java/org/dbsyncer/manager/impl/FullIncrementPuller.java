/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.AbstractPuller;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.util.FullTableProgressUtil;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.spi.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 全量+增量同步：先捕获位点 → 全量 → 从位点启动增量
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-18 15:02
 */
@Component
public final class FullIncrementPuller extends AbstractPuller {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Set<String> running = new CopyOnWriteArraySet<>();

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private FullPuller fullPuller;

    @Resource
    private IncrementPuller incrementPuller;

    @Resource
    private LogService logService;

    @Resource
    private ClusterService clusterService;

    @Override
    public void start(Mapping mapping) {
        start(mapping, false);
    }

    @Override
    public void start(Mapping mapping, boolean autoRecovery) {
        final String metaId = mapping.getMetaId();
        running.add(metaId);
        Thread worker = new Thread(() -> runFullIncrementSync(mapping, metaId, autoRecovery));
        worker.setName("full-increment-worker-" + mapping.getId());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close(String metaId) {
        running.remove(metaId);
        fullPuller.close(metaId);
        incrementPuller.close(metaId);
    }

    @Override
    public boolean isActive(String metaId) {
        return running.contains(metaId) || fullPuller.isActive(metaId) || incrementPuller.isActive(metaId);
    }

    private void runFullIncrementSync(Mapping mapping, String metaId, boolean autoRecovery) {
        try {
            Meta meta = metaProfile.getMeta(metaId);
            if (ModelEnum.isIncrement(getFullIncrementPhase(meta))) {
                if (!clusterService.isStandalone() && !clusterService.isIncrementAssignedToLocal(mapping.getId())) {
                    return;
                }
                incrementPuller.start(mapping, autoRecovery);
                return;
            }
            prepareFullPhase(mapping, meta, metaId);
            logger.info("开始全量同步：{}, {}", metaId, mapping.getName());
            fullPuller.runSync(mapping, false);
            if (!isRunning(metaId)) {
                return;
            }
            if (clusterService.isLeader()) {
                markFullIncrementPhase(metaId, ModelEnum.INCREMENT.getCode());
                clusterService.assignIncrementMapping(mapping.getId());
            }
            if (!clusterService.isStandalone() && !clusterService.isIncrementAssignedToLocal(mapping.getId())) {
                logger.info("全量已完成，本节点不启动增量：{}", metaId);
                return;
            }
            if (incrementPuller.isActive(metaId)) {
                return;
            }
            logger.info("开始增量同步：{}, {}", metaId, mapping.getName());
            incrementPuller.start(mapping, autoRecovery);
        } catch (Exception e) {
            logger.error("全量+增量同步失败：{}, {}", metaId, e.getMessage(), e);
            logService.log(LogType.SystemLog.ERROR, e.getMessage());
            incrementPuller.close(metaId);
            publishClosedEvent(metaId);
        } finally {
            running.remove(metaId);
        }
    }

    private void prepareFullPhase(Mapping mapping, Meta meta, String metaId) {
        if (shouldResumeFullPhase(meta)) {
            logger.info("恢复全量阶段：{}, {}", metaId, mapping.getName());
            return;
        }
        //重新开始，获取增量位点信息
        incrementPuller.captureAndSaveOffset(mapping);
    }

    private boolean isRunning(String metaId) {
        return running.contains(metaId);
    }

    private String getFullIncrementPhase(Meta meta) {
        if (meta == null || meta.getSnapshot() == null) {
            return null;
        }
        return meta.getSnapshot().get(ParserEnum.FULL_INCREMENT_PHASE.getCode());
    }

    /**
     * 全量阶段未完成时，从 tableProgress 断点恢复，避免重置进度后 success 重复累加。
     */
    private boolean shouldResumeFullPhase(Meta meta) {
        String phase = getFullIncrementPhase(meta);
        if (StringUtil.isBlank(phase)) {
            return false;
        }
        long total = meta.getTotal().get();
        long processed = meta.getSuccess().get() + meta.getFail().get();
        if (total > 0 && processed >= total) {
            return false;
        }
        Map<String, String> snapshot = meta.getSnapshot();
        return FullTableProgressUtil.hasIncomplete(snapshot) || processed > 0;
    }

    /**
     * 标记全量+增量阶段并清理全量进度。
     */
    private void markFullIncrementPhase(String metaId, String phase) {
        Meta meta = metaProfile.getMeta(metaId);
        meta.getSnapshot().put(ParserEnum.FULL_INCREMENT_PHASE.getCode(), phase);
        FullTableProgressUtil.clear(meta.getSnapshot());
        FullTableProgressUtil.removeLegacyTaskBreakpointKeys(meta.getSnapshot());
        profileComponent.editConfigModel(meta);
    }
}
