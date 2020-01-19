package org.dbsyncer.common;

import org.apache.commons.lang.StringUtils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultThreadFactory implements ThreadFactory {

    /**
     *原子操作保证每个线程都有唯一的
     */
    private static final AtomicInteger threadNumber = new AtomicInteger(1);

    private final AtomicInteger mThreadNum = new AtomicInteger(1);

    private final String prefix;

    private final boolean daemoThread;

    private final ThreadGroup threadGroup;

    public DefaultThreadFactory() {
        this("listener-threadpool-" + threadNumber.getAndIncrement(), false);
    }

    public DefaultThreadFactory(String prefix) {
        this(prefix, false);
    }

    public DefaultThreadFactory(String prefix, boolean daemo) {
        this.prefix = StringUtils.isNotEmpty(prefix) ? prefix.concat("-thread-") : "";
        daemoThread = daemo;
        SecurityManager s = System.getSecurityManager();
        threadGroup = (s == null) ? Thread.currentThread().getThreadGroup() : s.getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String name = prefix + mThreadNum.getAndIncrement();
        Thread ret = new Thread(threadGroup, runnable, name, 0);
        ret.setDaemon(daemoThread);
        return ret;
    }
}
