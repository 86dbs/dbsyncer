package org.dbsyncer.common.util;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public abstract class ParallelTaskUtil {

    /**
     * 默认线程池大小
     */
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;

    /**
     * 默认任务超时时间（秒）
     */
    private static final int DEFAULT_TIMEOUT_SECONDS = 300;

    /**
     * 默认线程队列大小
     */
    private static final int DEFAULT_QUEUE_SIZE = 1024;

    /**
     * 创建线程池
     *
     * @param threadPoolSize
     * @param maximumPoolSize
     * @param queueSize
     * @return
     */
    public static ExecutorService createExecutor(int threadPoolSize, int maximumPoolSize, int queueSize) {
        return new ThreadPoolExecutor(
                threadPoolSize,                     // 核心线程数
                maximumPoolSize,                    // 最大线程数
                60L,                                // 线程空闲时间
                TimeUnit.SECONDS,                   // 过期时间单位
                new ArrayBlockingQueue<>(queueSize), // 有界队列
                Executors.defaultThreadFactory(),   // 线程工厂
                new ThreadPoolExecutor.CallerRunsPolicy());  // 拒绝策略, 当队列满时，由调用线程执行任务
    }

    /**
     * 并行处理列表数据
     *
     * @param dataList 数据列表
     * @param processor 数据处理函数
     * @param threadPoolSize 线程池大小
     * @param <T> 数据类型
     * @param <R> 结果类型
     * @return 处理结果列表
     */
    public static <T, R> List<R> submit(Collection<T> dataList, Processor<T, R> processor, int threadPoolSize, Logger logger) {
        if (CollectionUtils.isEmpty(dataList)) {
            return Collections.emptyList();
        }

        ExecutorService executor = createExecutor(threadPoolSize, threadPoolSize, DEFAULT_QUEUE_SIZE);
        List<Future<R>> futures = new ArrayList<>();
        List<R> results = new ArrayList<>();

        try {
            // 提交所有任务
            for (T data : dataList) {
                futures.add(executor.submit(() -> {
                    try {
                        return processor.process(data);
                    } catch (Throwable e) {
                        logger.error("任务执行异常", e);
                        return null;
                    }
                }));
            }

            // 等待所有任务完成
            for (Future<R> future : futures) {
                try {
                    R result = future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (result != null) {
                        results.add(result);
                    }
                } catch (TimeoutException e) {
                    logger.error("任务执行超时", e);
                } catch (Exception e) {
                    logger.error("任务执行异常", e);
                }
            }
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        return results;
    }

    /**
     * 并行处理列表数据（无返回值）
     *
     * @param dataList 数据列表
     * @param consumer 数据处理消费者
     * @param logger 日志记录器
     * @param <T> 数据类型
     */
    public static <T> void execute(Collection<T> dataList, Consumer<T> consumer, Logger logger) {
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }

        ExecutorService executor = createExecutor(DEFAULT_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE, DEFAULT_QUEUE_SIZE);
        List<Future<?>> futures = new ArrayList<>();

        try {
            // 提交所有任务
            for (T data : dataList) {
                futures.add(executor.submit(() -> {
                    try {
                        consumer.accept(data);
                    } catch (Throwable e) {
                        logger.error("任务执行异常", e);
                    }
                }));
            }

            // 等待所有任务完成
            for (Future<?> future : futures) {
                try {
                    future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    logger.error("任务执行超时", e);
                } catch (Exception e) {
                    logger.error("任务执行异常", e);
                }
            }
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 并行处理列表数据（无返回值）
     *
     * @param executor 自定义线程池
     * @param dataList 数据列表
     * @param consumer 数据处理消费者
     * @param logger 日志记录器
     * @param <T> 数据类型
     */
    public static <T> void execute(ExecutorService executor, Collection<T> dataList, Consumer<T> consumer, Logger logger) {
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }

        List<Future<?>> futures = new ArrayList<>();
        // 提交所有任务
        for (T data : dataList) {
            futures.add(executor.submit(() -> {
                try {
                    consumer.accept(data);
                } catch (Throwable e) {
                    logger.error("任务执行异常", e);
                }
            }));
        }

        // 等待所有任务完成
        for (Future<?> future : futures) {
            try {
                future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.error("任务执行异常", e);
            } catch (Exception e) {
                logger.error("任务执行异常", e);
            }
        }
    }

    /**
     * 数据处理接口
     *
     * @param <T> 输入类型
     * @param <R> 输出类型
     */
    @FunctionalInterface
    public interface Processor<T, R> {
        R process(T data) throws Exception;
    }

}