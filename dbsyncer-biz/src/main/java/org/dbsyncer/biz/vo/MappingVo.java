package org.dbsyncer.biz.vo;

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
    private ConnectorVo sourceConnector;
    private ConnectorVo targetConnector;

    public MappingVo(boolean running, ConnectorVo sourceConnector, ConnectorVo targetConnector) {
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

    public ConnectorVo getSourceConnector() {
        return sourceConnector;
    }

    public void setSourceConnector(ConnectorVo sourceConnector) {
        this.sourceConnector = sourceConnector;
    }

    public ConnectorVo getTargetConnector() {
        return targetConnector;
    }

    public void setTargetConnector(ConnectorVo targetConnector) {
        this.targetConnector = targetConnector;
    }
}