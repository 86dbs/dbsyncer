package org.dbsyncer.storage.binlog;

import java.io.Closeable;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/26 23:23
 */
public interface BinlogActuator extends Closeable {

    /**
     * 获取索引文件名
     *
     * @return
     */
    String getFileName();

    /**
     * 状态是否为运行中
     *
     * @return
     */
    boolean isRunning();

    /**
     * 标记为停止状态
     */
    void stop();
}