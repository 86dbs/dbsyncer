package org.dbsyncer.manager.puller;

import org.dbsyncer.common.model.Task;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ListenerConfig;

public interface Increment {

    void execute(Task task, ListenerConfig listenerConfig, Connector connector);

}
