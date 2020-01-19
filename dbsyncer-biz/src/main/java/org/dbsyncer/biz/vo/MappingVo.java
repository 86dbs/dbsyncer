package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/03 17:20
 */
public class MappingVo extends Mapping {

    // 是否运行
    private boolean running;

    // 连接器
    private Connector sourceConnector;
    private Connector targetConnector;

    public MappingVo(boolean running, Connector sourceConnector, Connector targetConnector) {
        this.running = running;
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public Connector getSourceConnector() {
        return sourceConnector;
    }

    public void setSourceConnector(Connector sourceConnector) {
        this.sourceConnector = sourceConnector;
    }

    public Connector getTargetConnector() {
        return targetConnector;
    }

    public void setTargetConnector(Connector targetConnector) {
        this.targetConnector = targetConnector;
    }
}