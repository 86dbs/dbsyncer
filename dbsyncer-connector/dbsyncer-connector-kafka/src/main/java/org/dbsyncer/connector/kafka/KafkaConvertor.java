/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka消息格式化转换器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-08-05 15:30
 */
public class KafkaConvertor {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 格式化消息为标准格式
     *
     * @param context 插件上下文
     * @param data 原始数据
     * @param pkFields 主键字段列表
     * @return 格式化后的数据
     */
    public List<Map> formatMessages(PluginContext context, List<Map> data, List<Field> pkFields) {
        List<Map> formattedData = new ArrayList<>();
        
        // 获取元数据信息
        String dataSourceType = context.getSourceConnectorInstance().getConfig().getConnectorType();
        String sourceAddress = getSourceAddressFromSource(context);
        String sourceDBName = getSourceDBNameFromSource(context);
        String targetTableName = context.getTargetTableName();
        String event = context.getEvent();
        long timestamp = System.currentTimeMillis();
        
        for (Map<String, Object> originalData : data) {
            Map<String, Object> formattedMessage = createFormattedMessage(
                originalData, dataSourceType, sourceAddress, sourceDBName, 
                targetTableName, event, timestamp, pkFields);
            formattedData.add(formattedMessage);
        }
        
        return formattedData;
    }

    /**
     * 从源连接器配置中提取源地址
     */
    public String getSourceAddressFromSource(PluginContext context) {
        try {
            ConnectorInstance sourceConnectorInstance = context.getSourceConnectorInstance();
            if (sourceConnectorInstance != null) {
                Method getConfigMethod = sourceConnectorInstance.getClass().getMethod("getConfig");
                Object config = getConfigMethod.invoke(sourceConnectorInstance);

                // 尝试获取URL属性
                try {
                    Method getUrlMethod = config.getClass().getMethod("getUrl");
                    Object url = getUrlMethod.invoke(config);
                    if (url != null) {
                        // 从URL中提取主机地址
                        String urlStr = url.toString();
                        // 简单的主机提取逻辑，实际可以根据需要增强
                        if (urlStr.contains("//")) {
                            String hostPort = urlStr.split("//")[1];
                            if (hostPort.contains("/")) {
                                hostPort = hostPort.split("/")[0];
                            }
                            if (hostPort.contains(":")) {
                                return hostPort.split(":")[0];
                            }
                            return hostPort;
                        }
                        return urlStr;
                    }
                } catch (NoSuchMethodException e) {
                    // 如果没有getUrl方法，尝试其他方法
                    logger.debug("源连接器配置中没有getUrl方法");
                }
            }
        } catch (Exception e) {
            logger.warn("无法从源连接器获取源地址", e);
        }
        return "UNKNOWN";
    }

    /**
     * 从源连接器配置中提取数据库名
     */
    public String getSourceDBNameFromSource(PluginContext context) {
        try {
            ConnectorInstance sourceConnectorInstance = context.getSourceConnectorInstance();
            if (sourceConnectorInstance != null) {
                Method getConfigMethod = sourceConnectorInstance.getClass().getMethod("getConfig");
                Object config = getConfigMethod.invoke(sourceConnectorInstance);

                // 尝试获取schema属性
                try {
                    Method getSchemaMethod = config.getClass().getMethod("getSchema");
                    Object schema = getSchemaMethod.invoke(config);
                    if (schema != null) {
                        return schema.toString();
                    }
                } catch (NoSuchMethodException e) {
                    logger.debug("源连接器配置中没有getSchema方法");
                }

                // 尝试获取URL属性并从中提取数据库名
                try {
                    Method getUrlMethod = config.getClass().getMethod("getUrl");
                    Object url = getUrlMethod.invoke(config);
                    if (url != null) {
                        String urlStr = url.toString();
                        // 简单的数据库名提取逻辑，实际可以根据需要增强
                        if (urlStr.contains("//")) {
                            String path = urlStr.split("//")[1];
                            if (path.contains("/")) {
                                String dbPart = path.split("/")[1];
                                if (dbPart.contains("?")) {
                                    dbPart = dbPart.split("\\?")[0];
                                }
                                return dbPart;
                            }
                        }
                    }
                } catch (NoSuchMethodException e) {
                    logger.debug("源连接器配置中没有getUrl方法");
                }
            }
        } catch (Exception e) {
            logger.warn("无法从源连接器获取数据库名", e);
        }
        return "UNKNOWN";
    }

    /**
     * 创建符合要求格式的消息
     *
     * @param data            原始数据
     * @param dataSourceType  数据源类型
     * @param sourceAddress   源地址
     * @param sourceDBName    源数据库名
     * @param targetTableName 目标表名
     * @param event           事件类型
     * @param timestamp       时间戳
     * @param pkFields        主键字段列表
     * @return 格式化后的消息
     */
    private Map<String, Object> createFormattedMessage(Map<String, Object> data, String dataSourceType,
                                                       String sourceAddress, String sourceDBName, String targetTableName, String event, long timestamp, List<Field> pkFields) {
        Map<String, Object> message = new LinkedHashMap<>();

        // 添加元数据信息，从源连接器自动获取
        message.put("dataSourceType", dataSourceType);
        message.put("sourceAddress", sourceAddress);
        message.put("sourceDBName", sourceDBName);
        message.put("tableName", targetTableName);
        message.put("type", "DML");
        message.put("subType", event);
        message.put("value", data);
        message.put("eventTimestamp", timestamp);
        message.put("collectTimestamp", timestamp);

        // 添加主键信息
        Map<String, Object> primaryKeys = new LinkedHashMap<>();
        for (Field field : pkFields) {
            primaryKeys.put(field.getName(), data.get(field.getName()));
        }
        message.put("primaryKeys", primaryKeys);

        return message;
    }

    /**
     * 格式化DDL消息为标准格式
     *
     * @param ddlConfig      DDL配置信息
     * @param tableName      表名
     * @param sourceConnectorType 源连接器类型
     * @param sourceAddress  源地址
     * @param sourceDBName   源数据库名
     * @return 格式化后的DDL消息
     */
    public Map<String, Object> formatDDLMessage(DDLConfig ddlConfig, String tableName,
                                                String sourceConnectorType, String sourceAddress, String sourceDBName) {
        Map<String, Object> message = new LinkedHashMap<>();
        long timestamp = System.currentTimeMillis();

        // 添加元数据信息
        message.put("dataSourceType", sourceConnectorType);
        message.put("sourceAddress", sourceAddress);
        message.put("sourceDBName", sourceDBName);
        message.put("tableName", tableName);
        message.put("type", "DDL");
        
        // 设置subType为DDL操作类型
        DDLOperationEnum operation = ddlConfig.getDdlOperationEnum();
        if (operation != null) {
            message.put("subType", operation.name());
        } else {
            message.put("subType", "UNKNOWN");
        }
        
        // 添加DDL SQL
        message.put("ddlSql", ddlConfig.getSql());
        message.put("operation", operation != null ? operation.name() : "UNKNOWN");
        
        // 添加字段变更信息
        message.put("addedFieldNames", ddlConfig.getAddedFieldNames() != null ? ddlConfig.getAddedFieldNames() : new ArrayList<>());
        message.put("modifiedFieldNames", ddlConfig.getModifiedFieldNames() != null ? ddlConfig.getModifiedFieldNames() : new ArrayList<>());
        message.put("droppedFieldNames", ddlConfig.getDroppedFieldNames() != null ? ddlConfig.getDroppedFieldNames() : new ArrayList<>());
        message.put("changedFieldNames", ddlConfig.getChangedFieldNames() != null ? ddlConfig.getChangedFieldNames() : new LinkedHashMap<>());
        
        // 添加时间戳
        message.put("eventTimestamp", timestamp);
        message.put("collectTimestamp", timestamp);

        return message;
    }
}