package org.dbsyncer.listener.quartz;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.AbstractExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private List<Map<String, String>> commands;
    private int commandSize;

    private int readNum;
    private String eventFieldName;
    private Set<String> update;
    private Set<String> insert;
    private Set<String> delete;
    private String taskKey;
    private long period;
    private AtomicBoolean running;

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
        commandSize = commands.size();

        readNum = listenerConfig.getReadNum();
        eventFieldName = listenerConfig.getEventFieldName();
        update = Stream.of(listenerConfig.getUpdate().split(",")).collect(Collectors.toSet());
        insert = Stream.of(listenerConfig.getInsert().split(",")).collect(Collectors.toSet());
        delete = Stream.of(listenerConfig.getDelete().split(",")).collect(Collectors.toSet());

        taskKey = UUIDUtil.getUUID();
        period = listenerConfig.getPeriod();
        running = new AtomicBoolean();
        run();
        scheduledTaskService.start(taskKey, period * 1000, this);
        logger.info("启动定时任务:{} >> {}秒", taskKey, period);
    }

    @Override
    public void run() {
        try {
            if (running.compareAndSet(false, true)) {
                // 依次执行同步映射关系
                for (int i = 0; i < commandSize; i++) {
                    execute(commands.get(i), i);
                }
                running.compareAndSet(true, false);
            }
        } catch (Exception e) {
            running.compareAndSet(true, false);
            errorEvent(e);
            logger.error(e.getMessage());
        }
    }

    @Override
    public void close() {
        scheduledTaskService.stop(taskKey);
    }

    private void execute(Map<String, String> command, int index) {
        // 检查增量点
        ConnectorMapper connectionMapper = connectorFactory.connect(connectorConfig);
        Point point = checkLastPoint(command, index);
        int pageIndex = 1;
        for (; ; ) {
            Result reader = connectorFactory.reader(connectionMapper, new ReaderConfig(point.getCommand(), point.getArgs(), pageIndex++, readNum));
            List<Map> data = reader.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }

            Object event = null;
            for (Map<String, Object> row : data) {
                event = row.get(eventFieldName);
                if (update.contains(event)) {
                    changedEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_MAP, row));
                    continue;
                }
                if (insert.contains(event)) {
                    changedEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_MAP, row));
                    continue;
                }
                if (delete.contains(event)) {
                    changedEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_DELETE, row, Collections.EMPTY_MAP));
                    continue;
                }

            }
            // 更新记录点
            point.refresh();

        }

        // 持久化
        if (point.refreshed()) {
            snapshot.putAll(point.getPosition());
        }

    }

    public void setCommands(List<Map<String, String>> commands) {
        this.commands = commands;
    }

}