package org.dbsyncer.manager.puller;

import org.dbsyncer.common.model.Task;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ListenerConfig;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-11 22:44
 */
public abstract class AbstractIncrement implements Increment {

    /**
     * 启动
     */
    protected abstract void run(ListenerConfig listenerConfig, Connector connector);

    /**
     * 关闭
     */
    protected abstract void close();

    @Override
    public void execute(Task task, ListenerConfig listenerConfig, Connector connector) {
        // 注册关闭监听事件
        task.attachClosedEvent(() -> close());

        // 启动
        run(listenerConfig, connector);
    }

}
