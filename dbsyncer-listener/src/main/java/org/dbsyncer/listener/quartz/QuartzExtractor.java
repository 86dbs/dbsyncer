package org.dbsyncer.listener.quartz;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.QuartzFilter;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
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
    private String taskKey;
    private String cron;
    private AtomicBoolean running;

    @Override
    public void start() {
        init();
        run();
        scheduledTaskService.start(taskKey, cron, this);
        logger.info("启动定时任务:{} >> {}", taskKey, cron);
    }

    @Override
    public void run() {
        try {
            logger.debug("执行定时任务:{} >> {}", taskKey, cron);
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
        scheduledTaskService.stop(taskKey);
    }

    private void execute(Map<String, String> command, int index) {
        // 检查增量点
        Point point = checkLastPoint(command, index);
        int pageIndex = 1;
        for (; ; ) {
            Result reader = connectorFactory.reader(connectorConfig, point.getCommand(), point.getArgs(), pageIndex++, readNum);
            List<Map> data = reader.getData();
            if (CollectionUtils.isEmpty(data)) {
                break;
            }

            Object event = null;
            for (Map<String, Object> row : data) {
                event = row.get(eventFieldName);
                if (update.contains(event)) {
                    changedQuartzEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_MAP, row));
                    continue;
                }
                if (insert.contains(event)) {
                    changedQuartzEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_MAP, row));
                    continue;
                }
                if (delete.contains(event)) {
                    changedQuartzEvent(new RowChangedEvent(index, ConnectorConstant.OPERTION_DELETE, row, Collections.EMPTY_MAP));
                    continue;
                }

            }
            // 更新记录点
            point.refresh();

        }

        // 持久化
        if (point.refreshed()) {
            map.putAll(point.getPosition());
            logger.info("增量点：{}", map);
        }

    }

    private Point checkLastPoint(Map<String, String> command, int index) {
        // 检查是否存在系统参数
        final String query = command.get(ConnectorConstant.OPERTION_QUERY);
        List<QuartzFilterEnum> filterEnums = Stream.of(QuartzFilterEnum.values()).filter(f -> {
            Assert.isTrue(appearNotMoreThanOnce(query, f.getType()), String.format("系统参数%s存在多个.", f.getType()));
            return StringUtils.contains(query, f.getType());
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(filterEnums)) {
            return new Point(command, new ArrayList<>());
        }

        Point point = new Point();
        // 存在系统参数，替换
        String replaceQuery = query;
        for (QuartzFilterEnum quartzFilter : filterEnums) {
            final String type = quartzFilter.getType();
            final QuartzFilter f = quartzFilter.getQuartzFilter();

            // 替换字符
            replaceQuery = StringUtils.replace(replaceQuery, "'" + type + "'", "?");

            // 创建参数索引key
            final String key = index + type;

            // 开始位置
            if(f.begin()){
                if (!map.containsKey(key)) {
                    final Object val = f.getObject();
                    point.addArg(val);
                    map.put(key, f.toString(val));
                    continue;
                }

                // 读取历史增量点
                Object val = f.getObject(map.get(key));
                point.addArg(val);
                point.setBeginKey(key);
                point.setBeginValue(f.toString(f.getObject()));
                continue;
            }
            // 结束位置(刷新)
            Object val = f.getObject();
            point.addArg(val);
            point.setBeginValue(f.toString(val));
        }
        point.setCommand(ConnectorConstant.OPERTION_QUERY, replaceQuery);

        return point;
    }

    private void init() {
        commandSize = commands.size();

        readNum = listenerConfig.getReadNum();
        eventFieldName = listenerConfig.getEventFieldName();
        update = Stream.of(listenerConfig.getUpdate().split(",")).collect(Collectors.toSet());
        insert = Stream.of(listenerConfig.getInsert().split(",")).collect(Collectors.toSet());
        delete = Stream.of(listenerConfig.getDelete().split(",")).collect(Collectors.toSet());

        taskKey = UUIDUtil.getUUID();
        cron = listenerConfig.getCronExpression();
        running = new AtomicBoolean();
    }

    private boolean appearNotMoreThanOnce(String str, String searchStr) {
        return StringUtils.indexOf(str, searchStr) == StringUtils.lastIndexOf(str, searchStr);
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

    final class Point {

        private Map<String, String> position;
        private Map<String, String> command;
        private List<Object> args;
        private String beginKey;
        private String beginValue;
        private boolean refreshed;

        public Point() {
            this.position = new HashMap<>();
            this.command = new HashMap<>();
            this.args = new ArrayList<>();
        }

        public Point(Map<String, String> command, List<Object> args) {
            this.command = command;
            this.args = args;
        }

        public void setCommand(String key, String value) {
            command.put(key, value);
        }

        public void addArg(Object val) {
            args.add(val);
        }

        public void refresh() {
            if(StringUtils.isNotBlank(beginKey) && StringUtils.isNotBlank(beginValue)){
                position.put(beginKey, beginValue);
                refreshed = true;
            }
        }

        public boolean refreshed() {
            return refreshed;
        }

        public Map<String, String> getPosition() {
            return position;
        }

        public Map<String, String> getCommand() {
            return command;
        }

        public List<Object> getArgs() {
            return new ArrayList<>(args);
        }

        public void setBeginKey(String beginKey) {
            this.beginKey = beginKey;
        }

        public void setBeginValue(String beginValue) {
            this.beginValue = beginValue;
        }
    }

}