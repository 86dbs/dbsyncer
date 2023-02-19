package org.dbsyncer.listener.quartz;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.dbsyncer.listener.AbstractExtractor;
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
public abstract class AbstractQuartzExtractor extends AbstractExtractor implements ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String CURSOR = "cursor";
    private static final int READ_NUM = 1000;
    private List<TableGroupCommand> commands;
    private String eventFieldName;
    private boolean forceUpdate;
    private Set<String> update;
    private Set<String> insert;
    private Set<String> delete;
    private String taskKey;
    private volatile boolean running;
    private final Lock lock = new ReentrantLock(true);

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
            logger.error(e.getMessage());
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

    private void execute(TableGroupCommand tableGroupCommand, int index) {
        final Map<String, String> command = tableGroupCommand.getCommand();
        final Set<String> primaryKeys = tableGroupCommand.getPrimaryKeys();

        // 检查增量点
        ConnectorMapper connectionMapper = connectorFactory.connect(connectorConfig);
        Point point = checkLastPoint(command, index);
        int pageIndex = 1;
        Object[] cursors = PrimaryKeyUtil.getLastCursors(snapshot.get(index + CURSOR));

        while (running) {
            Result reader = connectorFactory.reader(connectionMapper, new ReaderConfig(point.getCommand(), point.getArgs(), cursors, pageIndex++, READ_NUM));
            List<Map> data = reader.getSuccessData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }

            Object event = null;
            for (Map<String, Object> row : data) {
                if (forceUpdate) {
                    changedEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_UPDATE, row));
                    continue;
                }

                event = row.get(eventFieldName);
                if (update.contains(event)) {
                    changedEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_UPDATE, row));
                    continue;
                }
                if (insert.contains(event)) {
                    changedEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_INSERT, row));
                    continue;
                }
                if (delete.contains(event)) {
                    changedEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_DELETE, row));
                    continue;
                }

            }
            // 更新记录点
            cursors = PrimaryKeyUtil.getLastCursors(data, primaryKeys);
            point.refresh();

            if (data.size() < READ_NUM) {
                break;
            }
        }

        // 持久化
        if (point.refreshed()) {
            snapshot.putAll(point.getPosition());
            snapshot.put(index + CURSOR, StringUtil.join(cursors, ","));
        }

    }

    public void setCommands(List<TableGroupCommand> commands) {
        this.commands = commands;
    }

}