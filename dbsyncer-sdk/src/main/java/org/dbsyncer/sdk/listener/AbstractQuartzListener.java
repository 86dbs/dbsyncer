package org.dbsyncer.sdk.listener;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.event.ScanChangedEvent;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.quartz.Point;
import org.dbsyncer.sdk.quartz.TableGroupQuartzCommand;
import org.dbsyncer.sdk.scheduled.ScheduledTaskJob;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final int READ_NUM = 1000;
    private List<TableGroupQuartzCommand> commands;
    private String eventFieldName;
    private boolean forceUpdate;
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
        forceUpdate = StringUtil.isBlank(listenerConfig.getEventFieldName());
        update = Stream.of(listenerConfig.getUpdate().split(",")).collect(Collectors.toSet());
        insert = Stream.of(listenerConfig.getInsert().split(",")).collect(Collectors.toSet());
        delete = Stream.of(listenerConfig.getDelete().split(",")).collect(Collectors.toSet());

        taskKey = UUIDUtil.getUUID();
        running = true;

        scheduledTaskService.start(taskKey, listenerConfig.getCron(), this);
        logger.info("启动定时任务:{} >> {}", taskKey, listenerConfig.getCron());
    }

    @Override
    public void run() {
        final Lock taskLock = lock;
        boolean locked = false;
        try {
            locked = taskLock.tryLock();
            if (locked) {
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

    private void execute(TableGroupQuartzCommand tableGroupQuartzCommand, int index) {
        final Map<String, String> command = tableGroupQuartzCommand.getCommand();
        final List<String> primaryKeys = tableGroupQuartzCommand.getPrimaryKeys();
        final Table table = tableGroupQuartzCommand.getTable();
        boolean supportedCursor = StringUtil.isNotBlank(command.get(ConnectorConstant.OPERTION_QUERY_CURSOR));

        // 检查增量点
        Point point = checkLastPoint(command, index);
        int pageIndex = 1;
        Object[] cursors = PrimaryKeyUtil.getLastCursors(snapshot.get(index + CURSOR));

        while (running) {
            ReaderConfig readerConfig = new ReaderConfig(table, point.getCommand(), point.getArgs(), supportedCursor, cursors, pageIndex++, READ_NUM);
            Result reader = connectorService.reader(connectorInstance, readerConfig);
            List<Map> data = reader.getSuccessData();
            if (CollectionUtils.isEmpty(data)) {
                cursors = new Object[0];
                break;
            }

            for (Map<String, Object> row : data) {
                if (forceUpdate) {
                    changeEvent(new ScanChangedEvent(index, ConnectorConstant.OPERTION_UPDATE, row));
                    continue;
                }

                Object eventValue = row.get(eventFieldName);
                if (update.contains(eventValue)) {
                    changeEvent(new ScanChangedEvent(index, ConnectorConstant.OPERTION_UPDATE, row));
                    continue;
                }
                if (insert.contains(eventValue)) {
                    changeEvent(new ScanChangedEvent(index, ConnectorConstant.OPERTION_INSERT, row));
                    continue;
                }
                if (delete.contains(eventValue)) {
                    changeEvent(new ScanChangedEvent(index, ConnectorConstant.OPERTION_DELETE, row));
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
        if (supportedCursor) {
            snapshot.put(index + CURSOR, StringUtil.join(cursors, ","));
        }

    }

    public void setCommands(List<TableGroupQuartzCommand> commands) {
        this.commands = commands;
    }

}