package org.dbsyncer.manager.impl;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.dbsyncer.manager.AbstractPuller;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.ParserEnum;
import org.dbsyncer.parser.event.FullRefreshEvent;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 全量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public final class FullPuller extends AbstractPuller implements ApplicationListener<FullRefreshEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    private Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void start(Mapping mapping) {
        Thread worker = new Thread(() -> {
            final String metaId = mapping.getMetaId();
            ExecutorService executor = null;
            try {
                List<TableGroup> list = profileComponent.getSortedTableGroupAll(mapping.getId());
                Assert.notEmpty(list, "映射关系不能为空");
                logger.info("开始全量同步：{}, {}", metaId, mapping.getName());
                map.putIfAbsent(metaId, new Task(metaId));
                executor = Executors.newFixedThreadPool(mapping.getThreadNum());
                Task task = map.get(metaId);
                doTask(task, mapping, list, executor);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                logService.log(LogType.SystemLog.ERROR, e.getMessage());
            } finally {
                try {
                    if (executor != null) {
                        executor.shutdown();
                    }
                } catch (Exception e) {
                    logService.log(LogType.SystemLog.ERROR, e.getMessage());
                }
                map.remove(metaId);
                publishClosedEvent(metaId);
                logger.info("结束全量同步：{}, {}", metaId, mapping.getName());
            }
        });
        worker.setName(new StringBuilder("full-worker-").append(mapping.getId()).toString());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close(String metaId) {
        Task task = map.get(metaId);
        if (null != task) {
            task.stop();
        }
    }

    @Override
    public void onApplicationEvent(FullRefreshEvent event) {
        // 异步监听任务刷新事件
        flush(event.getTask());
    }

    private void doTask(Task task, Mapping mapping, List<TableGroup> list, Executor executor) {
        // 记录开始时间
        long now = Instant.now().toEpochMilli();
        task.setBeginTime(now);
        task.setEndTime(now);

        // 获取上次同步点
        Meta meta = profileComponent.getMeta(task.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        task.setPageIndex(NumberUtil.toInt(snapshot.get(ParserEnum.PAGE_INDEX.getCode()), ParserEnum.PAGE_INDEX.getDefaultValue()));
        // 反序列化游标值类型(通常为数字或字符串类型)
        String cursorValue = snapshot.get(ParserEnum.CURSOR.getCode());
        task.setCursors(PrimaryKeyUtil.getLastCursors(cursorValue));
        task.setTableGroupIndex(NumberUtil.toInt(snapshot.get(ParserEnum.TABLE_GROUP_INDEX.getCode()), ParserEnum.TABLE_GROUP_INDEX.getDefaultValue()));
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
        snapshot.put(ParserEnum.CURSOR.getCode(), StringUtil.join(task.getCursors(), ","));
        snapshot.put(ParserEnum.TABLE_GROUP_INDEX.getCode(), String.valueOf(task.getTableGroupIndex()));
        profileComponent.editConfigModel(meta);
    }

}