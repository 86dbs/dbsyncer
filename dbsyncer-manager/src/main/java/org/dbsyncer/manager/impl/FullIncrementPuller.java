/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.AbstractPuller;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.enums.ModelEnum;
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
    private FullPuller fullPuller;

    @Resource
    private IncrementPuller incrementPuller;

    @Resource
    private LogService logService;

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

    private void runFullIncrementSync(Mapping mapping, String metaId, boolean autoRecovery) {
        try {
            Meta meta = profileComponent.getMeta(metaId);
            if (ModelEnum.isIncrement(getFullIncrementPhase(meta))) {
                //重启恢复是如果是增量阶段直接启动增量任务
                incrementPuller.start(mapping, autoRecovery);
                return;
            }
            prepareFullPhase(mapping, meta, metaId);
            logger.info("开始全量同步：{}, {}", metaId, mapping.getName());
            fullPuller.runSync(mapping, false);
            if (!isRunning(metaId)) {
                return;
            }
            markFullIncrementPhase(metaId, ModelEnum.INCREMENT.getCode());
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
        return meta.getSnapshot().get(ParserEnum.FULL_INCREMENT_PHASE.getCode());
    }

    /**
     * 全量阶段未完成时，从 snapshot 断点恢复，避免重置进度后 success 重复累加
     */
    private boolean shouldResumeFullPhase(Meta meta) {
        String phase = getFullIncrementPhase(meta);
        //如果是空直接表示全量增量都没有跑
        if (StringUtil.isBlank(phase)) {
            return false;
        }
        long total = meta.getTotal().get();
        long processed = meta.getSuccess().get() + meta.getFail().get();
        if (total > 0 && processed >= total) {
            return false;
        }

        Map<String, String> snapshot = meta.getSnapshot();
        int tableGroupIndex = NumberUtil.toInt(snapshot.get(ParserEnum.TABLE_GROUP_INDEX.getCode()), ParserEnum.TABLE_GROUP_INDEX.getDefaultValue());
        int pageIndex = NumberUtil.toInt(snapshot.get(ParserEnum.PAGE_INDEX.getCode()), ParserEnum.PAGE_INDEX.getDefaultValue());
        String cursor = snapshot.get(ParserEnum.CURSOR.getCode());

        return tableGroupIndex > ParserEnum.TABLE_GROUP_INDEX.getDefaultValue()
                || pageIndex > ParserEnum.PAGE_INDEX.getDefaultValue()
                || StringUtil.isNotBlank(cursor)
                || processed > 0;
    }

    /**
     * 标记状态
     */
    private void markFullIncrementPhase(String metaId, String phase) {
        Meta meta = profileComponent.getMeta(metaId);
        meta.getSnapshot().put(ParserEnum.FULL_INCREMENT_PHASE.getCode(), phase);

        //清除全量标记
        meta.getSnapshot().remove(ParserEnum.PAGE_INDEX.getCode());
        meta.getSnapshot().remove(ParserEnum.CURSOR.getCode());
        meta.getSnapshot().remove(ParserEnum.TABLE_GROUP_INDEX.getCode());
        profileComponent.editConfigModel(meta);
    }
}
