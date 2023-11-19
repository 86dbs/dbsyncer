package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.ReaderConfig;
import org.dbsyncer.sdk.config.WriterBatchConfig;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;

import java.util.List;
import java.util.Map;

/**
 * 连接器基础功能
 *
 * @param <I> ConnectorInstance
 * @param <C> ConnectorConfig
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-19 23:24
 */
public interface ConnectorService<I, C> {

    /**
     * 建立连接
     *
     * @param config
     * @return
     */
    ConnectorInstance connect(C config);

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
     * @param config
     * @return
     */
    String getConnectorInstanceCacheKey(C config);

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
     * @param config
     * @return
     */
    Result reader(I connectorInstance, ReaderConfig config);

    /**
     * 批量写入目标源数据
     *
     * @param connectorInstance
     * @param config
     * @return
     */
    Result writer(I connectorInstance, WriterBatchConfig config);

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
}