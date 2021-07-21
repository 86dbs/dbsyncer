package org.dbsyncer.connector;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.config.*;

import java.util.List;
import java.util.Map;

/**
 * 连接器基础功能
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/18 23:30
 */
public interface Connector {

    /**
     * 建立连接
     *
     * @param config
     * @return
     */
    ConnectorMapper connect(ConnectorConfig config);

    /**
     * 断开连接
     *
     * @param connectorMapper
     */
    void disconnect(ConnectorMapper connectorMapper);

    /**
     * 检查连接器是否连接正常
     *
     * @param connectorMapper
     * @return
     */
    boolean isAlive(ConnectorMapper connectorMapper);

    /**
     * 获取连接缓存key
     *
     * @param config
     * @return
     */
    String getConnectorMapperCacheKey(ConnectorConfig config);

    /**
     * 获取所有表名
     *
     * @param connectorMapper
     * @return
     */
    List<String> getTable(ConnectorMapper connectorMapper);

    /**
     * 获取表元信息
     *
     * @param connectorMapper
     * @param tableName
     * @return
     */
    MetaInfo getMetaInfo(ConnectorMapper connectorMapper, String tableName);

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
     * 获取总数
     *
     * @param config
     * @param command
     * @return
     */
    long getCount(ConnectorMapper config, Map<String, String> command);

    /**
     * 分页获取数据源数据
     *
     * @param config
     * @return
     */
    Result reader(ReaderConfig config);

    /**
     * 批量写入目标源数据
     *
     * @param config
     * @return
     */
    Result writer(WriterBatchConfig config);

    /**
     * 写入目标源数据
     *
     * @param config
     * @return
     */
    Result writer(WriterSingleConfig config);

}