package org.dbsyncer.parser.event;

import org.dbsyncer.listener.model.ChangedOffset;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

import java.util.List;

/**
 * 刷新偏移量事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-08-23 22:45
 */
public final class RefreshOffsetEvent extends ApplicationContextEvent {

    private List<ChangedOffset> offsetList;

    /**
     * Create a new ContextStartedEvent.
     *
     * @param source the {@code ApplicationContext} that the event is raised for
     *               (must not be {@code null})
     */
    public RefreshOffsetEvent(ApplicationContext source, List<ChangedOffset> offsetList) {
        super(source);
        this.offsetList = offsetList;
    }

    public List<ChangedOffset> getOffsetList() {
        return offsetList;
    }
}