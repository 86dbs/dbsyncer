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
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.MetaContext;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 连接器基础功能
 *
 * @param <I> ConnectorInstance
 * @param <C> ConnectorConfig
 * @author AE86
 * @version 1.0.0
 * @date 2023-11-19 23:24
 */
public interface ConnectorService<I extends ConnectorInstance, C extends ConnectorConfig> {

    /**
     * 连接器类型
     */
    String getConnectorType();

    /**
     * 可扩展的表类型
     */
    TableTypeEnum getExtendedTableType();

    /**
     * 获取配置对象
     */
    Class<C> getConfigClass();

    /**
     * 建立连接
     */
    ConnectorInstance connect(C connectorConfig, ConnectorServiceContext context);

    /**
     * 连接器配置校验器
     */
    ConfigValidator getConfigValidator();

    /**
     * 断开连接
     */
    void disconnect(I connectorInstance);

    /**
     * 检查连接器是否连接正常
     */
    boolean isAlive(I connectorInstance);

    /**
     * 获取所有的数据库
     */
    default List<String> getDatabases(I connectorInstance) {
        return Collections.emptyList();
    }

    /**
     * 获取指定数据库名的Schema
     */
    default List<String> getSchemas(I connectorInstance, String catalog) {
        return Collections.emptyList();
    }

    /**
     * 获取所有表名
     */
    List<Table> getTable(I connectorInstance, ConnectorServiceContext context);

    /**
     * 获取表元信息
     */
    List<MetaInfo> getMetaInfo(I connectorInstance, ConnectorServiceContext context);

    /**
     * 获取总数
     */
    long getCount(I connectorInstance, MetaContext metaContext);

    /**
     * 分页获取数据源数据
     */
    Result reader(I connectorInstance, ReaderContext context);

    /**
     * 批量写入目标源数据
     */
    Result writer(I connectorInstance, PluginContext context);

    /**
     * 执行DDL命令
     */
    default Result writerDDL(I connectorInstance, DDLConfig ddlConfig) {
        throw new SdkException("Unsupported method.");
    }

    /**
     * 获取数据源同步参数
     */
    Map<String, String> getSourceCommand(CommandConfig commandConfig);

    /**
     * 获取目标源同步参数
     */
    Map<String, String> getTargetCommand(CommandConfig commandConfig);

    /**
     * 获取监听器
     *
     * @param listenerType {@link ListenerTypeEnum}
     */
    Listener getListener(String listenerType);

    /**
     * 获取存储服务
     */
    default StorageService getStorageService() {
        return null;
    }

    /**
     * 获取标准数据类型解析器
     */
    SchemaResolver getSchemaResolver();

    /**
     * 获取指定时间的位点信息
     */
    default Object getPosition(I connectorInstance) {
        return StringUtil.EMPTY;
    }
}
