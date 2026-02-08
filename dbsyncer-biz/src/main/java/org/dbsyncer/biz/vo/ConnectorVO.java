package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/10 17:20
 */
public class ConnectorVO extends Connector {

    // 是否运行
    private boolean running;

    public ConnectorVO(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
