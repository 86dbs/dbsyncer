package org.dbsyncer.connector.kafka.model;

import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KafkaMessage {
    
    /**
     * 数据源类型（连接器类型）
     */
    private String dataSourceType;
    
    /**
     * 源地址
     */
    private String sourceAddress;
    
    /**
     * 源数据库名
     */
    private String sourceDBName;
    
    /**
     * 表名
     */
    private String tableName;
    
    /**
     * 消息类型：DML 或 DDL
     */
    private String type;
    
    /**
     * 子类型：DML时为事件类型（INSERT/UPDATE/DELETE），DDL时为操作类型（ALTER_ADD等）
     */
    private String subType;
    
    /**
     * 事件时间戳
     */
    private long eventTimestamp;
    
    /**
     * 采集时间戳
     */
    private long collectTimestamp;
    
    // ========== DML 特有字段 ==========
    
    /**
     * 数据值（DML消息）
     */
    private Map<String, Object> value;
    
    /**
     * 主键信息（DML消息）
     */
    private Map<String, Object> primaryKeys;
    
    // ========== DDL 特有字段 ==========
    
    /**
     * DDL SQL（DDL消息）
     */
    private String ddlSql;
    
    /**
     * DDL操作类型（DDL消息）
     */
    private String operation;
    
    /**
     * 新增字段名列表（DDL消息）
     */
    private List<String> addedFieldNames;
    
    /**
     * 修改字段名列表（DDL消息）
     */
    private List<String> modifiedFieldNames;
    
    /**
     * 删除字段名列表（DDL消息）
     */
    private List<String> droppedFieldNames;
    
    /**
     * 重命名字段映射（DDL消息）：旧字段名 -> 新字段名
     */
    private Map<String, String> changedFieldNames;
    
    /**
     * 创建DML消息
     *
     * @param data            原始数据
     * @param dataSourceType  数据源类型
     * @param sourceAddress   源地址
     * @param sourceDBName    源数据库名
     * @param targetTableName 目标表名
     * @param event           事件类型
     * @param timestamp       时间戳
     * @param pkFields        主键字段列表
     * @return DML消息对象
     */
    public static KafkaMessage createDMLMessage(Map<String, Object> data, String dataSourceType,
                                                String sourceAddress, String sourceDBName, String targetTableName,
                                                String event, long timestamp, List<Field> pkFields) {
        KafkaMessage message = new KafkaMessage();
        
        // 设置公共字段
        message.setDataSourceType(dataSourceType);
        message.setSourceAddress(sourceAddress);
        message.setSourceDBName(sourceDBName);
        message.setTableName(targetTableName);
        message.setType("DML");
        message.setSubType(event);
        message.setEventTimestamp(timestamp);
        message.setCollectTimestamp(timestamp);
        
        // 设置DML特有字段
        message.setValue(data);
        
        // 添加主键信息
        Map<String, Object> primaryKeys = new LinkedHashMap<>();
        for (Field field : pkFields) {
            primaryKeys.put(field.getName(), data.get(field.getName()));
        }
        message.setPrimaryKeys(primaryKeys);
        
        return message;
    }
    
    /**
     * 创建DDL消息
     *
     * @param ddlConfig          DDL配置信息
     * @param tableName          表名
     * @param sourceConnectorType 源连接器类型
     * @param sourceAddress      源地址
     * @param sourceDBName       源数据库名
     * @return DDL消息对象
     */
    public static KafkaMessage createDDLMessage(DDLConfig ddlConfig, String tableName,
                                                String sourceConnectorType, String sourceAddress, String sourceDBName) {
        KafkaMessage message = new KafkaMessage();
        long timestamp = System.currentTimeMillis();
        
        // 设置公共字段
        message.setDataSourceType(sourceConnectorType);
        message.setSourceAddress(sourceAddress);
        message.setSourceDBName(sourceDBName);
        message.setTableName(tableName);
        message.setType("DDL");
        message.setEventTimestamp(timestamp);
        message.setCollectTimestamp(timestamp);
        
        // 设置subType为DDL操作类型
        DDLOperationEnum operation = ddlConfig.getDdlOperationEnum();
        if (operation != null) {
            message.setSubType(operation.name());
            message.setOperation(operation.name());
        } else {
            message.setSubType("UNKNOWN");
            message.setOperation("UNKNOWN");
        }
        
        // 设置DDL特有字段
        message.setDdlSql(ddlConfig.getSql());
        message.setAddedFieldNames(ddlConfig.getAddedFieldNames() != null ? ddlConfig.getAddedFieldNames() : new ArrayList<>());
        message.setModifiedFieldNames(ddlConfig.getModifiedFieldNames() != null ? ddlConfig.getModifiedFieldNames() : new ArrayList<>());
        message.setDroppedFieldNames(ddlConfig.getDroppedFieldNames() != null ? ddlConfig.getDroppedFieldNames() : new ArrayList<>());
        message.setChangedFieldNames(ddlConfig.getChangedFieldNames() != null ? ddlConfig.getChangedFieldNames() : new LinkedHashMap<>());
        
        return message;
    }
    
    /**
     * 转换为Map格式（用于发送到Kafka）
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        
        // 公共字段
        map.put("dataSourceType", dataSourceType);
        map.put("sourceAddress", sourceAddress);
        map.put("sourceDBName", sourceDBName);
        map.put("tableName", tableName);
        map.put("type", type);
        map.put("subType", subType);
        map.put("eventTimestamp", eventTimestamp);
        map.put("collectTimestamp", collectTimestamp);
        
        // DML 特有字段
        if ("DML".equals(type)) {
            if (value != null) {
                map.put("value", value);
            }
            if (primaryKeys != null) {
                map.put("primaryKeys", primaryKeys);
            }
        }
        
        // DDL 特有字段
        if ("DDL".equals(type)) {
            if (ddlSql != null) {
                map.put("ddlSql", ddlSql);
            }
            if (operation != null) {
                map.put("operation", operation);
            }
            map.put("addedFieldNames", addedFieldNames != null ? addedFieldNames : new ArrayList<>());
            map.put("modifiedFieldNames", modifiedFieldNames != null ? modifiedFieldNames : new ArrayList<>());
            map.put("droppedFieldNames", droppedFieldNames != null ? droppedFieldNames : new ArrayList<>());
            map.put("changedFieldNames", changedFieldNames != null ? changedFieldNames : new LinkedHashMap<>());
        }
        
        return map;
    }
    
    // Getters and Setters
    
    public String getDataSourceType() {
        return dataSourceType;
    }
    
    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }
    
    public String getSourceAddress() {
        return sourceAddress;
    }
    
    public void setSourceAddress(String sourceAddress) {
        this.sourceAddress = sourceAddress;
    }
    
    public String getSourceDBName() {
        return sourceDBName;
    }
    
    public void setSourceDBName(String sourceDBName) {
        this.sourceDBName = sourceDBName;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getSubType() {
        return subType;
    }
    
    public void setSubType(String subType) {
        this.subType = subType;
    }
    
    public long getEventTimestamp() {
        return eventTimestamp;
    }
    
    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }
    
    public long getCollectTimestamp() {
        return collectTimestamp;
    }
    
    public void setCollectTimestamp(long collectTimestamp) {
        this.collectTimestamp = collectTimestamp;
    }
    
    public Map<String, Object> getValue() {
        return value;
    }
    
    public void setValue(Map<String, Object> value) {
        this.value = value;
    }
    
    public Map<String, Object> getPrimaryKeys() {
        return primaryKeys;
    }
    
    public void setPrimaryKeys(Map<String, Object> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
    
    public String getDdlSql() {
        return ddlSql;
    }
    
    public void setDdlSql(String ddlSql) {
        this.ddlSql = ddlSql;
    }
    
    public String getOperation() {
        return operation;
    }
    
    public void setOperation(String operation) {
        this.operation = operation;
    }
    
    public List<String> getAddedFieldNames() {
        return addedFieldNames;
    }
    
    public void setAddedFieldNames(List<String> addedFieldNames) {
        this.addedFieldNames = addedFieldNames;
    }
    
    public List<String> getModifiedFieldNames() {
        return modifiedFieldNames;
    }
    
    public void setModifiedFieldNames(List<String> modifiedFieldNames) {
        this.modifiedFieldNames = modifiedFieldNames;
    }
    
    public List<String> getDroppedFieldNames() {
        return droppedFieldNames;
    }
    
    public void setDroppedFieldNames(List<String> droppedFieldNames) {
        this.droppedFieldNames = droppedFieldNames;
    }
    
    public Map<String, String> getChangedFieldNames() {
        return changedFieldNames;
    }
    
    public void setChangedFieldNames(Map<String, String> changedFieldNames) {
        this.changedFieldNames = changedFieldNames;
    }
}
