package org.dbsyncer.listener.quartz;

public interface ScheduledTaskService {

    void start(ScheduledTask task);

    void stop(String taskKey);

}