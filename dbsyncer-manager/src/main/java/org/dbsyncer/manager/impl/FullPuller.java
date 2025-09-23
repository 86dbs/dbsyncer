/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.ProcessEvent;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.Task;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
                Task task = mapping.getTask();
                if (task == null) {
                    task = new Task(metaId);
                    mapping.setTask(task);
                }
                logger.info("开始全量同步：{}, {}", metaId, mapping.getName());
                doTask(task, mapping, list, executor);
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
                mapping.setTask(null);
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
        if (mapping != null) {
            Task task = mapping.getTask();
            if (task != null) {
                task.stop();
                mapping.setTask(null);
            }
        }
        mapping.resetMetaState();
    }

    private void doTask(Task task, Mapping mapping, List<TableGroup> list, Executor executor) {
        // 记录开始时间
        long now = Instant.now().toEpochMilli();
        task.setBeginTime(now);
        task.setEndTime(now);

        // 获取上次同步点
        Meta meta = profileComponent.getMeta(task.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        task.setPageIndex(NumberUtil.toInt(snapshot.get(ParserEnum.PAGE_INDEX.getCode()),
                ParserEnum.PAGE_INDEX.getDefaultValue()));
        // 反序列化游标值类型(通常为数字或字符串类型)
        task.setCursors(PrimaryKeyUtil.getLastCursors(snapshot.get(ParserEnum.CURSOR.getCode())));
        task.setTableGroupIndex(NumberUtil.toInt(snapshot.get(ParserEnum.TABLE_GROUP_INDEX.getCode()),
                ParserEnum.TABLE_GROUP_INDEX.getDefaultValue()));
        flush(task);

        int i = task.getTableGroupIndex();
        while (i < list.size()) {
            parserComponent.execute(task, mapping, list.get(i), executor);
            if (!task.isRunning()) {
                break;
            }
            task.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
            task.setCursors(null);
            task.setTableGroupIndex(++i);
            flush(task);
        }
        // 记录结束时间
        task.setEndTime(Instant.now().toEpochMilli());
        task.setTableGroupIndex(ParserEnum.TABLE_GROUP_INDEX.getDefaultValue());
        flush(task);

        // 检查并执行 Meta 中的阶段处理方法
        Runnable phaseHandler = meta.getPhaseHandler();
        if (phaseHandler != null) {
            phaseHandler.run();
        } else {
            meta.resetState();
        }
    }

    private void flush(Task task) {
        Meta meta = profileComponent.getMeta(task.getId());
        Assert.notNull(meta, "检查meta为空.");

        // 全量的过程中，有新数据则更新总数
        long finished = meta.getSuccess().get() + meta.getFail().get();
        if (meta.getTotal().get() < finished) {
            meta.getTotal().set(finished);
        }

        meta.setBeginTime(task.getBeginTime());
        meta.setEndTime(task.getEndTime());
        meta.setUpdateTime(Instant.now().toEpochMilli());
        Map<String, String> snapshot = meta.getSnapshot();
        snapshot.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(task.getPageIndex()));
        snapshot.put(ParserEnum.CURSOR.getCode(),
                StringUtil.getIfBlank(StringUtil.join(task.getCursors(), StringUtil.COMMA), StringUtil.EMPTY));
        snapshot.put(ParserEnum.TABLE_GROUP_INDEX.getCode(), String.valueOf(task.getTableGroupIndex()));
        profileComponent.editConfigModel(meta);
    }

    @Override
    public void taskFinished(String metaId) {
        Meta meta = profileComponent.getMeta(metaId);
        Mapping mapping = profileComponent.getMapping(meta.getMappingId());
        if (mapping != null) {
            Task task = mapping.getTask();
            if (task != null) {
                flush(task);
            }
        }
    }
}