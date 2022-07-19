package org.dbsyncer.storage.binlog;

import org.dbsyncer.storage.binlog.proto.BinlogMessage;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/8 23:34
 */
public interface BinlogRecorder {

    /**
     * 获取任务名称
     *
     * @return
     */
    default String getTaskName() {
        return getClass().getSimpleName();
    }

    /**
     * 将任务序列化刷入磁盘
     *
     * @param message
     */
    void flush(BinlogMessage message) throws IOException;

    /**
     * 消息同步完成后，删除消息记录
     *
     * @param messageIds
     */
    void complete(List<String> messageIds);

    /**
     * 获取缓存队列
     *
     * @return
     */
    Queue getQueue();

    /**
     * 获取缓存队列容量
     *
     * @return
     */
    int getQueueCapacity();

}