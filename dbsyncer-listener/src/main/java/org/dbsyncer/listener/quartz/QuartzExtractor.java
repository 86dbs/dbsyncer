package org.dbsyncer.listener.quartz;

import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.listener.AbstractExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private List<String> tableNames;
    private String key;
    private AtomicBoolean running = new AtomicBoolean();

    @Override
    public void start() {
        key = UUIDUtil.getUUID();
        String cron = listenerConfig.getCronExpression();
        logger.info("启动定时任务:{} >> {}", key, cron);
        scheduledTaskService.start(key, cron, this);
    }

    @Override
    public void run() {
        if(running.compareAndSet(false, true)){
            // TODO 获取tableGroup
            Map<String, String> command = null;
            int pageIndex = 1;
            int pageSize = 20;
            connectorFactory.reader(connectorConfig, command, pageIndex, pageSize);
        }
    }

    @Override
    public void close() {
        scheduledTaskService.stop(key);
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

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }
}