package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Field;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:00
 */
public abstract class AbstractPluginContext extends AbstractBaseContext implements PluginContext, Cloneable {

    /**
     * 是否终止任务
     * <p>true：目标源不再接收同步数据，默认值false
     */
    private boolean terminated;

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
     * 目标字段
     */
    private List<Field> targetFields;

    /**
     * 批量处理任务数
     */
    private int batchSize;

    /**
     * 是否覆盖更新
     */
    private boolean forceUpdate;

    /**
     * 是否启用字段解析器
     */
    private boolean enableSchemaResolver;

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

    @Override
    public boolean isTerminated() {
        return terminated;
    }

    @Override
    public void setTerminated(boolean terminated) {
        this.terminated = terminated;
    }

    @Override
    public ConnectorInstance getTargetConnectorInstance() {
        return targetConnectorInstance;
    }

    public void setTargetConnectorInstance(ConnectorInstance targetConnectorInstance) {
        this.targetConnectorInstance = targetConnectorInstance;
    }

    @Override
    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    @Override
    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    @Override
    public String getEvent() {
        return event;
    }

    @Override
    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public List<Field> getTargetFields() {
        return targetFields;
    }

    public void setTargetFields(List<Field> targetFields) {
        this.targetFields = targetFields;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public boolean isForceUpdate() {
        return forceUpdate;
    }

    public void setForceUpdate(boolean forceUpdate) {
        this.forceUpdate = forceUpdate;
    }

    @Override
    public boolean isEnableSchemaResolver() {
        return enableSchemaResolver;
    }

    public void setEnableSchemaResolver(boolean enableSchemaResolver) {
        this.enableSchemaResolver = enableSchemaResolver;
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

    public void setPluginExtInfo(String pluginExtInfo) {
        this.pluginExtInfo = pluginExtInfo;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}