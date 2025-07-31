package org.dbsyncer.plugin.impl;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.PluginService;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public final class KafkaMessageFormatterPlugin implements PluginService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private AppConfig appConfig;

    @Override
    public void convert(PluginContext context) {
        try {
            // 检查目标连接器是否为Kafka并且启用了共享Topic
            ConnectorInstance targetConnectorInstance = context.getTargetConnectorInstance();
            
            // 通过反射检查是否是Kafka连接器实例
            if (isKafkaConnectorInstance(targetConnectorInstance)) {
                // 获取Kafka配置
                Object kafkaConfig = getKafkaConfig(targetConnectorInstance);
                
                // 检查是否启用了共享Topic
                if (isShareTopicEnabled(kafkaConfig)) {
                    // 创建符合要求格式的消息
                    List<Map> targetList = context.getTargetList();
                    for (int i = 0; i < targetList.size(); i++) {
                        Map<String, Object> originalData = targetList.get(i);
                        Map<String, Object> formattedMessage = createFormattedMessage(originalData, context);
                        targetList.set(i, formattedMessage);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Kafka消息格式化插件处理异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 检查连接器实例是否为Kafka连接器实例
     */
    private boolean isKafkaConnectorInstance(ConnectorInstance connectorInstance) {
        return connectorInstance != null && 
               "Kafka".equals(connectorInstance.getClass().getSimpleName().replace("ConnectorInstance", ""));
    }

    /**
     * 获取Kafka配置对象
     */
    private Object getKafkaConfig(ConnectorInstance connectorInstance) throws Exception {
        Method getConfigMethod = connectorInstance.getClass().getMethod("getConfig");
        return getConfigMethod.invoke(connectorInstance);
    }

    /**
     * 检查是否启用了共享Topic
     */
    private boolean isShareTopicEnabled(Object kafkaConfig) throws Exception {
        try {
            Method isShareTopicMethod = kafkaConfig.getClass().getMethod("isShareTopic");
            return (Boolean) isShareTopicMethod.invoke(kafkaConfig);
        } catch (NoSuchMethodException e) {
            // 如果没有这个方法，说明是旧版本，返回false
            return false;
        }
    }

    /**
     * 自动从源连接器获取数据源类型
     */
    private String getDataSourceTypeFromSource(PluginContext context) {
        try {
            ConnectorInstance sourceConnectorInstance = context.getSourceConnectorInstance();
            if (sourceConnectorInstance != null) {
                // 从连接器实例类名中提取数据源类型
                String className = sourceConnectorInstance.getClass().getSimpleName();
                if (className.contains("ConnectorInstance")) {
                    String connectorType = className.replace("ConnectorInstance", "");
                    // 特殊处理SQL Server的情况
                    if ("SqlServer".equals(connectorType)) {
                        return "SQL_SERVER";
                    }
                    return connectorType.toUpperCase();
                }
            }
        } catch (Exception e) {
            logger.warn("无法从源连接器获取数据源类型", e);
        }
        return "UNKNOWN";
    }

    /**
     * 从源连接器配置中提取源地址
     */
    private String getSourceAddressFromSource(PluginContext context) {
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
    private String getSourceDBNameFromSource(PluginContext context) {
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
     * @param data 原始数据
     * @param context 插件上下文
     * @return 格式化后的消息
     */
    private Map<String, Object> createFormattedMessage(Map<String, Object> data, PluginContext context) {
        Map<String, Object> message = new LinkedHashMap<>();
        
        // 添加元数据信息，从源连接器自动获取
        message.put("dataSourceType", getDataSourceTypeFromSource(context));
        message.put("sourceAddress", getSourceAddressFromSource(context));
        message.put("sourceDBName", getSourceDBNameFromSource(context));
        message.put("tableName", context.getTargetTableName());
        message.put("type", "DML");
        message.put("subType", context.getEvent());
        message.put("value", data);
        long timestamp = System.currentTimeMillis();
        message.put("eventTimestamp", timestamp);
        message.put("collectTimestamp", timestamp);
        
        // 添加主键信息
        List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());
        Map<String, Object> primaryKeys = new LinkedHashMap<>();
        for (Field field : pkFields) {
            primaryKeys.put(field.getName(), data.get(field.getName()));
        }
        message.put("primaryKeys", primaryKeys);
        
        return message;
    }

    @Override
    public String getVersion() {
        return appConfig.getVersion();
    }

    @Override
    public String getName() {
        return "KafkaMessageFormatter";
    }
}