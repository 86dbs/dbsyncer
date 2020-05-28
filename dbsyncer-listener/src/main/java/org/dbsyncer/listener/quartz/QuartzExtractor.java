package org.dbsyncer.listener.quartz;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.ConnectorFactory;
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
 * 默认定时抽取
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 20:35
 */
public class QuartzExtractor extends AbstractExtractor implements ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private ConnectorFactory connectorFactory;
    private ScheduledTaskService scheduledTaskService;
    private List<Map<String, String>> commands;
    private int commandSize;

    private int readNum;
    private String eventFieldName;
    private Set<String> update;
    private Set<String> insert;
    private Set<String> delete;
    private String key;
    private String cron;
    private AtomicBoolean running;

    @Override
    public void start() {
        init();
        scheduledTaskService.start(key, cron, this);
        logger.info("启动定时任务:{} >> {}", key, cron);
    }

    @Override
    public void run() {
        try {
            logger.info("执行定时任务:{} >> {}", key, cron);
            if (running.compareAndSet(false, true)) {
                // 依次执行同步映射关系
                for (int i = 0; i < commandSize; i++) {
                    execute(commands.get(i), i);
                }
            }
            running.compareAndSet(true, false);
        } catch (Exception e) {
            running.compareAndSet(true, false);
            errorEvent(e);
            logger.error(e.getMessage());
        }
    }

    @Override
    public void close() {
        scheduledTaskService.stop(key);
    }

    private void execute(Map<String, String> command, int index) {
        int pageIndex = 1;
        for (; ; ) {
            Result reader = connectorFactory.reader(connectorConfig, command, pageIndex++, readNum);
            List<Map<String, Object>> data = reader.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }

            Object event = null;
            for (Map<String, Object> row : data) {
                event = row.get(eventFieldName);
                if (update.contains(event)) {
                    changedQuartzEvent(index, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_MAP, row);
                    continue;
                }
                if (insert.contains(event)) {
                    changedQuartzEvent(index, ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_MAP, row);
                    continue;
                }
                if (delete.contains(event)) {
                    changedQuartzEvent(index, ConnectorConstant.OPERTION_DELETE, row, Collections.EMPTY_MAP);
                    continue;
                }

            }
        }

    }

    private void init() {
        commandSize = commands.size();

        readNum = listenerConfig.getReadNum();
        eventFieldName = listenerConfig.getEventFieldName();
        update = Stream.of(listenerConfig.getUpdate().split(",")).collect(Collectors.toSet());
        insert = Stream.of(listenerConfig.getInsert().split(",")).collect(Collectors.toSet());
        delete = Stream.of(listenerConfig.getDelete().split(",")).collect(Collectors.toSet());

        key = UUIDUtil.getUUID();
        cron = listenerConfig.getCronExpression();
        running = new AtomicBoolean();
    }

    public void setConnectorFactory(ConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setScheduledTaskService(ScheduledTaskService scheduledTaskService) {
        this.scheduledTaskService = scheduledTaskService;
    }

    public void setCommands(List<Map<String, String>> commands) {
        this.commands = commands;
    }
}