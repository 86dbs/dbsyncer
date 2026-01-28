/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

import org.dbsyncer.connector.kafka.model.KafkaMessage;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
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
     * @param context  插件上下文
     * @param data     原始数据
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
            KafkaMessage message = KafkaMessage.createDMLMessage(
                    originalData, dataSourceType, sourceAddress, sourceDBName,
                    targetTableName, event, timestamp, pkFields);
            formattedData.add(message.toMap());
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
}