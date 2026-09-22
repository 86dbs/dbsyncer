package org.dbsyncer.common.event;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * 任务关闭事件
 *
 * @version 1.0.0
 * @author AE86
 * @date 2020-04-26 22:45
 */
public final class ClosedEvent extends ApplicationContextEvent {

    private final String metaId;

    public ClosedEvent(ApplicationContext source, String metaId) {
        super(source);
        this.metaId = metaId;
    }

    public String getMetaId() {
        return metaId;
    }
}
