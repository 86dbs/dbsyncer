/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-22 23:34
 */
public abstract class AbstractDispatchTask implements DispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, DispatchTask> active;

    private volatile boolean running;

    public abstract void execute() throws Exception;

    @Override
    public void run() {
        try {
            running = true;
            execute();
        } catch (Exception e) {
            logger.error("dispatch task，uniqueId:{}，error:{}", getUniqueId(), e);
        } finally {
            running = false;
            active.remove(getUniqueId());
        }
    }

    @Override
    public void destroy() {
        this.running = false;
    }

    protected boolean isRunning() {
        return running;
    }

    public void setActive(Map<String, DispatchTask> active) {
        this.active = active;
    }
}