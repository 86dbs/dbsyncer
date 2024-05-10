package org.dbsyncer.plugin;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.plugin.PluginContext;

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
    private ConnectorInstance sourceConnectorInstance;

    /**
     * 目标源连接实例
     */
    private ConnectorInstance targetConnectorInstance;

    /**
     * 数据源表
     */
    private String sourceTableName;

    /**
     * 目标源表
     */
    private String targetTableName;

    /**
     * 同步事件（INSERT/UPDATE/DELETE）
     */
    private String event;

    /**
     * 数据源数据集合
     */
    private List<Map> sourceList;

    /**
     * 目标源源数据集合
     */
    private List<Map> targetList;

    /**
     * 插件参数
     */
    private String pluginExtInfo;

    public void init(ConnectorInstance sourceConnectorInstance, ConnectorInstance targetConnectorInstance, String sourceTableName, String targetTableName, String event, List<Map> sourceList, List<Map> targetList, String pluginExtInfo) {
        this.sourceConnectorInstance = sourceConnectorInstance;
        this.targetConnectorInstance = targetConnectorInstance;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.event = event;
        this.sourceList = sourceList;
        this.targetList = targetList;
        this.pluginExtInfo = pluginExtInfo;
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
    public ConnectorInstance getSourceConnectorInstance() {
        return sourceConnectorInstance;
    }

    @Override
    public ConnectorInstance getTargetConnectorInstance() {
        return targetConnectorInstance;
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

    @Override
    public String getPluginExtInfo() {
        return pluginExtInfo;
    }
}