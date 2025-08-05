package org.dbsyncer.plugin.impl;

import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.connector.kafka.KafkaConnectorInstance;
import org.dbsyncer.connector.kafka.config.KafkaConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.PluginService;
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
            if (!(targetConnectorInstance instanceof KafkaConnectorInstance)) {
                return;
            }

            // 提前获取一次元数据信息，避免在循环中重复执行
            String dataSourceType = context.getSourceConnectorInstance().getConfig().getConnectorType();
            String sourceAddress = getSourceAddressFromSource(context);
            String sourceDBName = getSourceDBNameFromSource(context);
            String targetTableName = context.getTargetTableName();
            String event = context.getEvent();
            long timestamp = System.currentTimeMillis();

            // 提前提取主键字段列表
            List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(context.getTargetFields());

            // 创建符合要求格式的消息
            List<Map> targetList = context.getTargetList();
            for (int i = 0; i < targetList.size(); i++) {
                Map<String, Object> originalData = targetList.get(i);
                Map<String, Object> formattedMessage = createFormattedMessage(originalData, dataSourceType, sourceAddress,
                        sourceDBName, targetTableName, event, timestamp, pkFields);
                targetList.set(i, formattedMessage);
            }
        } catch (Exception e) {
            logger.error("Kafka消息格式化插件处理异常: {}", e.getMessage(), e);
        }
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

    @Override
    public String getVersion() {
        return appConfig.getVersion();
    }

    @Override
    public String getName() {
        return "KafkaMessageFormatter";
    }
}