package org.dbsyncer.web.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020-04-26 23:40
 */
@Configuration
public class TaskPoolConfig {

    /**
     * 工作线程池队列容量
     */
    @Value(value = "${dbsyncer.web.thread.pool.queue.capacity}")
    private int queueCapacity;

    @Bean("taskExecutor")
    public Executor taskExecutor() {
        //注意这一行日志：2. do submit,taskCount [101], completedTaskCount [87], activeCount [5], queueSize [9]
        //这说明提交任务到线程池的时候，调用的是submit(Callable task)这个方法，当前已经提交了101个任务，完成了87个，当前有5个线程在处理任务，还剩9个任务在队列中等待，线程池的基本情况一路了然；
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //核心线程数10：线程池创建时候初始化的线程数
        executor.setCorePoolSize(10);
        //最大线程数128：线程池最大的线程数，只有在缓冲队列满了之后才会申请超过核心线程数的线程
        //maxPoolSize 当系统负载大道最大值时,核心线程数已无法按时处理完所有任务,这是就需要增加线程.每秒200个任务需要20个线程,那么当每秒1000个任务时,则需要(1000-queueCapacity)*(20/200),即60个线程,可将maxPoolSize设置为60;
        executor.setMaxPoolSize(128);
        //缓冲队列：用来缓冲执行任务的队列
        executor.setQueueCapacity(queueCapacity);
        //允许线程的空闲时间30秒：当超过了核心线程出之外的线程在空闲时间到达之后会被销毁
        executor.setKeepAliveSeconds(30);
        //线程池名的前缀：设置好了之后可以方便我们定位处理任务所在的线程池
        executor.setThreadNamePrefix("taskExecutor");
        //理线程池对拒绝任务的处策略：这里采用了CallerRunsPolicy策略，当线程池没有处理能力的时候，该策略会直接在 execute 方法的调用线程中运行被拒绝的任务；如果执行程序已关闭，则会丢弃该任务
        /*CallerRunsPolicy：线程调用运行该任务的 execute 本身。此策略提供简单的反馈控制机制，能够减缓新任务的提交速度。
        这个策略显然不想放弃执行任务。但是由于池中已经没有任何资源了，那么就直接使用调用该execute的线程本身来执行。（开始我总不想丢弃任务的执行，但是对某些应用场景来讲，很有可能造成当前线程也被阻塞。如果所有线程都是不能执行的，很可能导致程序没法继续跑了。需要视业务情景而定吧。）
        AbortPolicy：处理程序遭到拒绝将抛出运行时 RejectedExecutionException
        这种策略直接抛出异常，丢弃任务。（jdk默认策略，队列满并线程满时直接拒绝添加新任务，并抛出异常，所以说有时候放弃也是一种勇气，为了保证后续任务的正常进行，丢弃一些也是可以接收的，记得做好记录）
        DiscardPolicy：不能执行的任务将被删除
        这种策略和AbortPolicy几乎一样，也是丢弃任务，只不过他不抛出异常。
        DiscardOldestPolicy：如果执行程序尚未关闭，则位于工作队列头部的任务将被删除，然后重试执行程序（如果再次失败，则重复此过程）
        该策略就稍微复杂一些，在pool没有关闭的前提下首先丢掉缓存在队列中的最早的任务，然后重新尝试运行该任务。这个策略需要适当小心*/
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }

}