package org.dbsyncer.common.util;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public abstract class ParallelProcessorUtil {

    /**
     * 默认线程池大小
     */
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;

    /**
     * 默认任务超时时间（秒）
     */
    private static final int DEFAULT_TIMEOUT_SECONDS = 300;

    /**
     * 并行处理列表数据
     *
     * @param dataList 数据列表
     * @param processor 数据处理函数
     * @param threadPoolSize 线程池大小
     * @param timeoutSeconds 超时时间（秒）
     * @param <T> 数据类型
     * @param <R> 结果类型
     * @return 处理结果列表
     */
    public static <T, R> List<R> processInParallel(List<T> dataList, Processor<T, R> processor, int threadPoolSize, int timeoutSeconds, Logger logger) {
        if (CollectionUtils.isEmpty(dataList)) {
            return Collections.emptyList();
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        List<Future<R>> futures = new ArrayList<>();
        List<R> results = new ArrayList<>();

        try {
            // 提交所有任务
            for (T data : dataList) {
                futures.add(executor.submit(() -> processor.process(data)));
            }

            // 等待所有任务完成
            for (Future<R> future : futures) {
                try {
                    R result = future.get(timeoutSeconds, TimeUnit.SECONDS);
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
     * @param threadPoolSize 线程池大小
     * @param timeoutSeconds 超时时间（秒）
     * @param <T> 数据类型
     */
    public static <T> void processInParallel(List<T> dataList, Consumer<T> consumer, int threadPoolSize, int timeoutSeconds, Logger logger) {
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        List<Future<?>> futures = new ArrayList<>();

        try {
            // 提交所有任务
            for (T data : dataList) {
                futures.add(executor.submit(() -> consumer.accept(data)));
            }

            // 等待所有任务完成
            for (Future<?> future : futures) {
                try {
                    future.get(timeoutSeconds, TimeUnit.SECONDS);
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
     * 并行处理列表数据（无返回值，使用默认配置）
     *
     * @param dataList 数据列表
     * @param consumer 数据处理消费者
     * @param logger 日志记录器
     * @param <T> 数据类型
     */
    public static <T> void processInParallel(List<T> dataList, Consumer<T> consumer, Logger logger) {
        processInParallel(dataList, consumer, DEFAULT_THREAD_POOL_SIZE, DEFAULT_TIMEOUT_SECONDS, logger);
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