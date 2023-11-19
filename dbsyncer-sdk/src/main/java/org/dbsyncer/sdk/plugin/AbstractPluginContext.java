package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.spi.ConnectorMapper;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:00
 */
public abstract class AbstractPluginContext implements PluginContext {

    /**
     * 是否终止任务
     * <p>true：目标源不再接收同步数据，默认值false
     */
    private boolean terminated;

    /**
     * 数据源连接实例
     */
    protected ConnectorMapper sourceConnectorMapper;

    /**
     * 目标源连接实例
     */
    protected ConnectorMapper targetConnectorMapper;

    /**
     * 数据源表
     */
    protected String sourceTableName;

    /**
     * 目标源表
     */
    protected String targetTableName;

    /**
     * 同步事件（INSERT/UPDATE/DELETE）
     */
    protected String event;

    /**
     * 数据源数据集合
     */
    protected List<Map> sourceList;

    /**
     * 目标源源数据集合
     */
    protected List<Map> targetList;

    public void init(ConnectorMapper sourceConnectorMapper, ConnectorMapper targetConnectorMapper, String sourceTableName, String targetTableName, String event,
                              List<Map> sourceList, List<Map> targetList) {
        this.sourceConnectorMapper = sourceConnectorMapper;
        this.targetConnectorMapper = targetConnectorMapper;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.event = event;
        this.sourceList = sourceList;
        this.targetList = targetList;
    }

    @Override
    public boolean isTerminated() {
        return terminated;
    }

    @Override
    public void setTerminated(boolean terminated) {
        this.terminated = terminated;
    }

    @Override
    public ConnectorMapper getSourceConnectorMapper() {
        return sourceConnectorMapper;
    }

    @Override
    public ConnectorMapper getTargetConnectorMapper() {
        return targetConnectorMapper;
    }

    @Override
    public String getSourceTableName() {
        return sourceTableName;
    }

    @Override
    public String getTargetTableName() {
        return targetTableName;
    }

    @Override
    public String getEvent() {
        return event;
    }

    @Override
    public List<Map> getSourceList() {
        return sourceList;
    }

    public void setSourceList(List<Map> sourceList) {
        this.sourceList = sourceList;
    }

    @Override
    public List<Map> getTargetList() {
        return targetList;
    }

    public void setTargetList(List<Map> targetList) {
        this.targetList = targetList;
    }
}