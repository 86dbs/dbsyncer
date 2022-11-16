package org.dbsyncer.parser.flush;

import java.util.Queue;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:34
 */
public interface BufferActuator {

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

    /**
     * 提交任务
     *
     * @param request
     * @return true/false
     */
    boolean offer(BufferRequest request);

}