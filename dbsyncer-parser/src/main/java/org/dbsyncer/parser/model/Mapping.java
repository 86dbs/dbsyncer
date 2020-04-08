package org.dbsyncer.parser.model;

import org.dbsyncer.connector.config.Field;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.parser.constant.ModelConstant;

import java.util.List;

/**
 * 驱动映射关系
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 13:19
 */
public class Mapping extends AbstractConfigModel {

    // 数据源连接器ID
    private String sourceConnectorId;

    // 目标源连接器ID
    private String targetConnectorId;

    // 数据源字段
    private List<Field> sourceColumn;

    // 目标源字段
    private List<Field> targetColumn;

    /**
     * 同步方式
     *
     * @see ModelConstant
     */
    private String model;

    // 监听配置
    private ListenerConfig listener;

    // 批量数
    private int batchNum = 200;

    // 线程数
    private int threadNum = 5;

    public String getSourceConnectorId() {
        return sourceConnectorId;
    }

    public Mapping setSourceConnectorId(String sourceConnectorId) {
        this.sourceConnectorId = sourceConnectorId;
        return this;
    }

    public String getTargetConnectorId() {
        return targetConnectorId;
    }

    public Mapping setTargetConnectorId(String targetConnectorId) {
        this.targetConnectorId = targetConnectorId;
        return this;
    }

    public List<Field> getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(List<Field> sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public List<Field> getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(List<Field> targetColumn) {
        this.targetColumn = targetColumn;
    }

    public String getModel() {
        return model;
    }

    public Mapping setModel(String model) {
        this.model = model;
        return this;
    }

    public ListenerConfig getListener() {
        return listener;
    }

    public Mapping setListener(ListenerConfig listener) {
        this.listener = listener;
        return this;
    }

    public int getBatchNum() {
        return batchNum;
    }

    public Mapping setBatchNum(int batchNum) {
        this.batchNum = batchNum;
        return this;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public Mapping setThreadNum(int threadNum) {
        this.threadNum = threadNum;
        return this;
    }

}