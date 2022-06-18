package org.dbsyncer.storage.binlog;

import org.dbsyncer.storage.binlog.proto.BinlogMessage;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/8 23:34
 */
public interface BinlogRecorder {

    /**
     * 将任务序列化刷入磁盘
     *
     * @param message
     */
    void flushBinlog(BinlogMessage message);

}