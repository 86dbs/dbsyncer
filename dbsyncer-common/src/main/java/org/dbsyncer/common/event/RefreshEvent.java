package org.dbsyncer.common.event;

import org.dbsyncer.common.model.Task;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

public class RefreshEvent extends ApplicationContextEvent {

    private Task task;

    /**
     * Create a new ContextStartedEvent.
     *
     * @param source the {@code ApplicationContext} that the event is raised for (must not be {@code null})
     */
    public RefreshEvent(ApplicationContext source, Task task) {
        super(source);
        this.task = task;
    }

    public Task getTask() {
        return task;
    }
}