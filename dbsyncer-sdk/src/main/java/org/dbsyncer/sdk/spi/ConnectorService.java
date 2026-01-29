/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.apache.commons.lang3.NotImplementedException;
import org.dbsyncer.common.model.Result;
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
import org.dbsyncer.sdk.parser.ddl.converter.IRToTargetConverter;
import org.dbsyncer.sdk.parser.ddl.converter.SourceToIRConverter;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.storage.StorageService;

import java.util.ArrayList;
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
    boolean isAlive(I connectorInstance) throws Exception;

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
    List<Table> getTable(I connectorInstance) throws Exception;

    /**
     * 获取表元信息
     *
     * @param connectorInstance
     * @param tableNamePattern
     * @return
     */
    MetaInfo getMetaInfo(I connectorInstance, String tableNamePattern) throws Exception;

    /**
     * 获取表元信息
     *
     * @param connectorInstance
     * @param tableNamePatterns
     * @return
     */
    default List<MetaInfo> getMetaInfos(I connectorInstance, List<String> tableNamePatterns) {
        return new ArrayList<>();
    }

    /**
     * 获取总数
     *
     * @param connectorInstance
     * @param command
     * @return
     */
    long getCount(I connectorInstance, Map<String, String> command) throws Exception;

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
     * 插入数据到目标源
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    default Result insert(I connectorInstance, PluginContext context) {
        throw new SdkException("should overwrite his method");
    }

    /**
     * 插入或更新数据到目标源（处理主键冲突）
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    default Result upsert(I connectorInstance, PluginContext context) {
        throw new SdkException("should overwrite his method");
    }

    /**
     * 更新目标源数据
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    default Result update(I connectorInstance, PluginContext context) {
        throw new SdkException("should overwrite his method");
    }

    /**
     * 删除目标源数据
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    default Result delete(I connectorInstance, PluginContext context) {
        throw new SdkException("should overwrite his method");
    }

    /**
     * 执行DDL命令
     *
     * @param connectorInstance 连接器实例
     * @param ddlConfig DDL配置
     * @param context 插件上下文，包含源连接器信息（可为null，数据库连接器不需要）
     * @return
     */
    default Result writerDDL(I connectorInstance, DDLConfig ddlConfig, org.dbsyncer.sdk.plugin.PluginContext context) {
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
    Map<String, String> getTargetCommand(CommandConfig commandConfig) throws Exception;

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
    default Map<String, String> getPosition(I connectorInstance) throws Exception {
        throw new NotImplementedException();
    }

    /**
     * 获取源到IR转换器
     *
     * @return 源到IR转换器
     */
    default SourceToIRConverter getSourceToIRConverter() {
        return null;
    }

    /**
     * 获取IR到目标转换器
     *
     * @return IR到目标转换器
     */
    default IRToTargetConverter getIRToTargetConverter() {
        return null;
    }

    /**
     * 基于源表结构生成目标表的 CREATE TABLE DDL
     *
     * @param sourceMetaInfo 源表元信息（字段为标准类型）
     * @param targetTableName 目标表名
     * @return CREATE TABLE DDL 语句
     * @throws UnsupportedOperationException 如果连接器不支持此功能
     */
    default String generateCreateTableDDL(MetaInfo sourceMetaInfo, String targetTableName) {
        throw new UnsupportedOperationException("该连接器不支持自动生成 CREATE TABLE DDL: " + getConnectorType());
    }

    /**
     * 判断连接器是否支持执行 DDL 操作（如表创建、表结构修改等）
     * 用于在表缺失检测时判断是否需要检查表是否存在
     * 
     * 注意：此方法判断的是连接器是否支持"写入/执行"DDL 操作，
     * 与 ListenerConfig.enableDDL（控制是否监听 DDL 事件）不同
     *
     * @return true 如果连接器支持执行 DDL 操作，false 否则
     */
    default boolean supportsDDLWrite() {
        // 默认返回 true，表示支持执行 DDL 操作
        // 对于不支持 DDL 操作的连接器（如 Kafka），需要重写此方法返回 false
        return true;
    }

    /**
     * 判断连接器是否支持创建表操作
     * 用于在表缺失检测时判断是否需要检查目标表是否存在
     * 
     * @return true 连接器支持创建表操作，false 不支持
     */
    default boolean supportsCreateTable() {
        // 默认返回 true，表示支持创建表操作
        // 对于不支持创建表的连接器（如 Kafka），需要重写此方法返回 false
        return true;
    }
}