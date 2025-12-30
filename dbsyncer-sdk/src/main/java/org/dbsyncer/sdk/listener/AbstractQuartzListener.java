/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.event.ScanChangedEvent;
import org.dbsyncer.sdk.model.Point;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.TableGroupQuartzCommand;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 定时抽取
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 20:35
 */
public abstract class AbstractQuartzListener extends AbstractListener implements ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String CURSOR = "cursor";
    private final int READ_NUM = 5000;
    private String mappingName;
    private List<TableGroupQuartzCommand> commands;
    private String eventFieldName;
    private boolean customEvent;
    private String event;
    private Set<String> update;
    private Set<String> insert;
    private Set<String> delete;
    private String taskKey;
    private volatile boolean running;
    private final Lock lock = new ReentrantLock();

    /**
     * 获取增量参数
     *
     * @param command
     * @param index
     * @return
     */
    protected abstract Point checkLastPoint(Map<String, String> command, int index);

    @Override
    public void start() {
        eventFieldName = listenerConfig.getEventFieldName();
        if (StringUtil.isBlank(eventFieldName)) {
            if (listenerConfig.isEnableUpdate()) {
                event = ConnectorConstant.OPERTION_UPDATE;
                customEvent = true;
            } else if (listenerConfig.isEnableInsert()) {
                event = ConnectorConstant.OPERTION_INSERT;
                customEvent = true;
            }
        }
        update = Stream.of(listenerConfig.getUpdate().split(",")).collect(Collectors.toSet());
        insert = Stream.of(listenerConfig.getInsert().split(",")).collect(Collectors.toSet());
        delete = Stream.of(listenerConfig.getDelete().split(",")).collect(Collectors.toSet());

        taskKey = UUIDUtil.getUUID();
        running = true;

        flushPoint();
        scheduledTaskService.start(taskKey, listenerConfig.getCron(), this);
        logger.info("启动定时任务:{}[{}]", mappingName, listenerConfig.getCron());
    }

    @Override
    public void run() {
        final Lock taskLock = lock;
        boolean locked = false;
        try {
            locked = taskLock.tryLock(3, TimeUnit.SECONDS);
            if (locked) {
                logger.info("执行定时任务:{}[{}]", mappingName, listenerConfig.getCron());
                for (int i = 0; i < commands.size(); i++) {
                    execute(commands.get(i), i);
                }
            }
        } catch (Exception e) {
            errorEvent(e);
            logger.error(e.getMessage(), e);
        } finally {
            if (locked) {
                taskLock.unlock();
            }
        }
    }

    @Override
    public void close() {
        scheduledTaskService.stop(taskKey);
        running = false;
    }

    private void flushPoint() {
        if (CollectionUtils.isEmpty(snapshot)) {
            for (int i = 0; i < commands.size(); i++) {
                final Map<String, String> command = commands.get(i).getCommand();
                // 更新最新增量点
                Point point = checkLastPoint(command, i);
                if (!CollectionUtils.isEmpty(point.getPosition())) {
                    snapshot.putAll(point.getPosition());
                }
            }
            super.forceFlushEvent();
        }
    }

    private void execute(TableGroupQuartzCommand cmd, int index) {
        final Map<String, String> command = cmd.getCommand();
        final List<String> primaryKeys = cmd.getPrimaryKeys();
        final Table table = cmd.getTable();

        // 检查增量点
        Point point = checkLastPoint(command, index);
        int pageIndex = 1;
        Object[] cursors = PrimaryKeyUtil.getLastCursors((String) snapshot.get(index + CURSOR));

        final QuartzListenerContext context = new QuartzListenerContext();
        context.setSourceConnectorInstance(connectorInstance);
        context.setTargetConnectorInstance(targetConnectorInstance);
        context.setSourceTable(table);
        context.setSourceTableName(table.getName());
        context.setTargetTableName(cmd.getTargetTable().getName());
        context.setCommand(point.getCommand());
        context.setSupportedCursor(StringUtil.isNotBlank(command.get(ConnectorConstant.OPERTION_QUERY_CURSOR)));
        context.setPageSize(READ_NUM);
        context.setPlugin(cmd.getPlugin());
        context.setPluginExtInfo(cmd.getPluginExtInfo());
        changeEventBefore(context);
        while (running) {
            context.setArgs(point.getArgs());
            context.setCursors(cursors);
            context.setPageIndex(pageIndex++);
            Result reader = connectorService.reader(connectorInstance, context);
            List<Map> data = reader.getSuccessData();
            if (CollectionUtils.isEmpty(data)) {
                cursors = new Object[0];
                break;
            }
            logger.info("{}[{}], data=[{}]", mappingName, context.getSourceTableName(), data.size());
            for (Map<String, Object> row : data) {
                if (customEvent) {
                    trySendEvent(new ScanChangedEvent(table.getName(), event, cmd.getChangedRow(row)));
                    continue;
                }

                Object eventValue = StringUtil.toString(row.get(eventFieldName));
                if (update.contains(eventValue)) {
                    trySendEvent(new ScanChangedEvent(table.getName(), ConnectorConstant.OPERTION_UPDATE, cmd.getChangedRow(row)));
                    continue;
                }
                if (insert.contains(eventValue)) {
                    trySendEvent(new ScanChangedEvent(table.getName(), ConnectorConstant.OPERTION_INSERT, cmd.getChangedRow(row)));
                    continue;
                }
                if (delete.contains(eventValue)) {
                    trySendEvent(new ScanChangedEvent(table.getName(), ConnectorConstant.OPERTION_DELETE, cmd.getChangedRow(row)));
                }
            }
            // 更新记录点
            cursors = PrimaryKeyUtil.getLastCursors(data, primaryKeys);
            point.refresh();

            if (data.size() < READ_NUM) {
                cursors = new Object[0];
                break;
            }
        }

        // 持久化
        if (point.refreshed()) {
            snapshot.putAll(point.getPosition());
        }
        if (context.isSupportedCursor() && cursors != null && cursors.length > 0) {
            snapshot.put(index + CURSOR, StringUtil.join(cursors, ","));
        }

    }

    private void trySendEvent(ChangedEvent event) {
        // 如果消费事件失败，重试
        while (running) {
            try {
                changeEvent(event);
                break;
            } catch (QueueOverflowException e) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        }
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }

    public void setCommands(List<TableGroupQuartzCommand> commands) {
        this.commands = commands;
    }

}