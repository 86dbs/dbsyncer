/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.common.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2025-06-22 23:34
 */
public abstract class AbstractDispatchTask implements DispatchTask, Cloneable {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Consumer<DispatchTask> consumer;

    private volatile boolean running;

    public abstract void execute() throws Exception;

    @Override
    public void run() {
        try {
            running = true;
            execute();
        } catch (Exception e) {
            logger.error("dispatch task，uniqueId:{}", getUniqueId(), e);
        } finally {
            running = false;
            if (consumer != null) {
                consumer.accept(this);
            }
        }
    }

    @Override
    public void destroy() {
        this.running = false;
    }

    @Override
    public void onDestroy(Consumer<DispatchTask> consumer) {
        this.consumer = consumer;
    }

    protected boolean isRunning() {
        return running;
    }

    @Override
    public AbstractDispatchTask clone() {
        try {
            return (AbstractDispatchTask) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
