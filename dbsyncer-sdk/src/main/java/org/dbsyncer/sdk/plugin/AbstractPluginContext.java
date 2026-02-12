package org.dbsyncer.sdk.plugin;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Plugin;
import org.dbsyncer.sdk.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:00
 */
public abstract class AbstractPluginContext extends AbstractBaseContext implements PluginContext, Cloneable {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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
     * 获取目标表信息
     */
    private Table targetTable;

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
     * 是否打印trace信息
     */
    private boolean enablePrintTraceInfo;

    /**
     * 数据源数据集合
     */
    private List<Map> sourceList;

    /**
     * 目标源源数据集合
     */
    private List<Map> targetList;

    /**
     * 插件
     */
    private Plugin plugin;

    /**
     * 插件参数
     */
    private String pluginExtInfo;

    private String traceId;

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
    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    @Override
    public String getSourceTableName() {
        logger.warn("方法已过时，请尽快替换为getSourceTable().getName()");
        return getSourceTable().getName();
    }

    @Override
    public String getTargetTableName() {
        logger.warn("方法已过时，请尽快替换为getTargetTable().getName()");
        return getTargetTable().getName();
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
    public boolean isEnablePrintTraceInfo() {
        return enablePrintTraceInfo;
    }

    public void setEnablePrintTraceInfo(boolean enablePrintTraceInfo) {
        this.enablePrintTraceInfo = enablePrintTraceInfo;
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
    public Plugin getPlugin() {
        return plugin;
    }

    public void setPlugin(Plugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String getPluginExtInfo() {
        return pluginExtInfo;
    }

    public void setPluginExtInfo(String pluginExtInfo) {
        this.pluginExtInfo = pluginExtInfo;
    }

    @Override
    public String getTraceId() {
        return traceId == null ? StringUtil.EMPTY : traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
