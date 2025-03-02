/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;

import java.util.List;
import java.util.Map;

/**
 * 连接器基础功能
 *
 * @param <I> ConnectorInstance
 * @param <C> ConnectorConfig
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-19 23:24
 */
public interface ConnectorService<I extends ConnectorInstance, C extends ConnectorConfig> {

    /**
     * 连接器类型
     */
    String getConnectorType();

    /**
     * 获取配置对象
     *
     * @return
     */
    Class<C> getConfigClass();

    /**
     * 建立连接
     *
     * @param connectorConfig
     * @return
     */
    ConnectorInstance connect(C connectorConfig);

    /**
     * 连接器配置校验器
     *
     * @return
     */
    ConfigValidator getConfigValidator();

    /**
     * 断开连接
     *
     * @param connectorInstance
     */
    void disconnect(I connectorInstance);

    /**
     * 检查连接器是否连接正常
     *
     * @param connectorInstance
     * @return
     */
    boolean isAlive(I connectorInstance);

    /**
     * 获取连接缓存key
     *
     * @param connectorConfig
     * @return
     */
    String getConnectorInstanceCacheKey(C connectorConfig);

    /**
     * 获取所有表名
     *
     * @param connectorInstance
     * @return
     */
    List<Table> getTable(I connectorInstance);

    /**
     * 获取表元信息
     *
     * @param connectorInstance
     * @param tableNamePattern
     * @return
     */
    MetaInfo getMetaInfo(I connectorInstance, String tableNamePattern);

    /**
     * 获取总数
     *
     * @param connectorInstance
     * @param command
     * @return
     */
    long getCount(I connectorInstance, Map<String, String> command);

    /**
     * 分页获取数据源数据
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    Result reader(I connectorInstance, ReaderContext context);

    /**
     * 批量写入目标源数据
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    Result writer(I connectorInstance, PluginContext context);

    /**
     * 执行DDL命令
     *
     * @param connectorInstance
     * @param ddlConfig
     * @return
     */
    default Result writerDDL(I connectorInstance, DDLConfig ddlConfig) {
        throw new SdkException("Unsupported method.");
    }

    /**
     * 获取数据源同步参数
     *
     * @param commandConfig
     * @return
     */
    Map<String, String> getSourceCommand(CommandConfig commandConfig);

    /**
     * 获取目标源同步参数
     *
     * @param commandConfig
     * @return
     */
    Map<String, String> getTargetCommand(CommandConfig commandConfig);

    /**
     * 获取监听器
     *
     * @param listenerType {@link ListenerTypeEnum}
     * @return
     */
    Listener getListener(String listenerType);

    /**
     * 获取存储服务
     *
     * @return
     */
    default StorageService getStorageService() {
        return null;
    }

    /**
     * 获取标准数据类型解析器
     *
     * @return
     */
    default SchemaResolver getSchemaResolver() {
        return null;
    }

    /**
     * 获取指定时间的位点信息
     *
     * @param connectorInstance
     * @return
     */
    default Object getPosition(I connectorInstance) {
        return StringUtil.EMPTY;
    }
}