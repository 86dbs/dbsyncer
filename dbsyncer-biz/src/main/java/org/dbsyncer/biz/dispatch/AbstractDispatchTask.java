/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-22 23:34
 */
public abstract class AbstractDispatchTask implements DispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Set<String> active;

    public abstract void execute() throws Exception;

    @Override
    public void run() {
        try{
            logger.info("dispatch task start, {}", getUniqueId());
            execute();
            logger.info("dispatch task finished, {}", getUniqueId());
        } catch (Exception e) {
            logger.error("dispatch task error", e);
        } finally {
            active.remove(getUniqueId());
        }
    }

    public void setActive(Set<String> active) {
        this.active = active;
    }
}