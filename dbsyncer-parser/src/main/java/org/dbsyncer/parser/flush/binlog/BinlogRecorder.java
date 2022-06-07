package org.dbsyncer.parser.flush.binlog;

import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.parser.flush.BufferRequest;

import java.io.File;
import java.util.Queue;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/8 0:53
 */
public class BinlogRecorder implements ScheduledTaskJob {

    private static final long PERIOD = 1000;

    private static final long BINLOG_MAX_SIZE = 512 * 1024 * 1024;

    private static final int BINLOG_EXPIRE_DAYS = 7;

    /**
     * 相对路径/data/binlog
     */
    private final String PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar)
            .append("data").append(File.separatorChar)
            .append("binlog").append(File.separatorChar)
            .toString();

    private String currentPath;

    private Queue buffer;

    private ScheduledTaskService scheduledTaskService;

    public BinlogRecorder(String fileName, Queue buffer, ScheduledTaskService scheduledTaskService) {
        currentPath = PATH + fileName;
        this.buffer = buffer;
        this.scheduledTaskService = scheduledTaskService;
        scheduledTaskService.start(PERIOD, this);

        //
    }

    @Override
    public void run() {
        // TODO 同步消息到缓存队列
    }

    public void offer(BufferRequest request) {
        // TODO 序列化消息
    }
}