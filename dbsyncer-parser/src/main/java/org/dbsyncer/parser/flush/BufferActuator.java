package org.dbsyncer.parser.flush;

import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:34
 */
public interface BufferActuator {

    /**
     * 提交任务
     *
     * @param request
     */
    void offer(BufferRequest request);

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
     * 获取线程池
     *
     * @return
     */
    Executor getExecutor();
}
