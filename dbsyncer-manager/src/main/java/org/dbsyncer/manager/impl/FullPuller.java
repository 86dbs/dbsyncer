/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.ProcessEvent;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 全量同步
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2020-04-26 15:28
 */
@Component
public final class FullPuller implements org.dbsyncer.manager.Puller, ProcessEvent {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Override
    public void start(Mapping mapping) {
        List<TableGroup> list = profileComponent.getSortedTableGroupAll(mapping.getId());
        Assert.notEmpty(list, "映射关系不能为空");
        Thread worker = new Thread(() -> {
            final String metaId = mapping.getMetaId();
            ExecutorService executor = Executors.newFixedThreadPool(mapping.getThreadNum());
            Meta meta = profileComponent.getMeta(metaId);
            assert meta != null;
            try {
                logger.info("开始全量同步：{}, {}", metaId, mapping.getName());
                doTask(metaId, mapping, list, executor);
            } catch (Exception e) {
                // 记录运行时异常状态和异常信息
                meta.saveState(MetaEnum.ERROR, e.getMessage());
                logger.error(e.getMessage(), e);
                logService.log(LogType.SystemLog.ERROR, e.getMessage());
            } finally {
                try {
                    executor.shutdown();
                } catch (Exception e) {
                    logService.log(LogType.SystemLog.ERROR, e.getMessage());
                }

                // 清除task引用
                if (meta.getPhaseHandler() == null && !meta.isError()) {
                    meta.resetState();
                }
                logger.info("结束全量同步：{}, {}", metaId, mapping.getName());
            }
        });
        worker.setName("full-worker-" + mapping.getId());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close(Mapping mapping) {
        mapping.resetMetaState();
    }

    private void doTask(String metaId, Mapping mapping, List<TableGroup> list, Executor executor) {
        // 获取Meta对象
        Meta meta = profileComponent.getMeta(metaId);

        // 并发处理所有TableGroup
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (TableGroup tableGroup : list) {
            if (tableGroup.isFullCompleted()) {
                continue;
            }
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    // 直接使用TableGroup的cursor进行流式处理
                    parserComponent.executeTableGroup(metaId, tableGroup, mapping, executor);
                } catch (Exception e) {
                    logger.error("TableGroup {} 处理失败", tableGroup.getIndex(), e);

                    // 记录TableGroup的错误状态
                    tableGroup.setErrorMessage(e.getMessage());
                    profileComponent.editConfigModel(tableGroup);

                    // 记录系统级错误
                    meta.saveState(MetaEnum.ERROR, e.getMessage());
                    logService.log(LogType.SystemLog.ERROR, e.getMessage());
                }
            }, executor);

            futures.add(future);
        }

        // 等待所有TableGroup完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // 记录结束时间
        flush(metaId);

        // 检查并执行 Meta 中的阶段处理方法
        Runnable phaseHandler = meta.getPhaseHandler();
        if (phaseHandler != null) {
            // 检查所有tableGroup是否都已完成
            boolean allTableGroupCompleted = true;
            for (TableGroup tableGroup : list) {
                if (!tableGroup.isFullCompleted()) {
                    allTableGroupCompleted = false;
                    break;
                }
            }
            
            // 只有当所有tableGroup都完成时才执行phaseHandler
            if (allTableGroupCompleted) {
                phaseHandler.run();
            } else {
                meta.resetState();
            }
        } else {
            meta.resetState();
        }
    }

    private void flush(String metaId) {
        Meta meta = profileComponent.getMeta(metaId);
        Assert.notNull(meta, "检查meta为空.");

        // 全量的过程中，有新数据则更新总数
        long finished = meta.getSuccess().get() + meta.getFail().get();
        if (meta.getTotal().get() < finished) {
            meta.getTotal().set(finished);
        }

        long epochMilli = Instant.now().toEpochMilli();
        meta.setEndTime(epochMilli);
        meta.setUpdateTime(epochMilli);
        profileComponent.editConfigModel(meta);
    }

    @Override
    public void taskFinished(String metaId) {
        Meta meta = profileComponent.getMeta(metaId);
        if (meta != null) {
            flush(metaId);
        }
    }
}