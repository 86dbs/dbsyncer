package org.dbsyncer.connector;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.config.CommandConfig;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.config.ReaderConfig;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;

import java.util.List;
import java.util.Map;

/**
 * 连接器基础功能
 *
 * @param <M> ConnectorMapper
 * @param <C> ConnectorConfig
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/18 23:30
 */
public interface Connector<M, C> {

    /**
     * 建立连接
     *
     * @param config
     * @return
     */
    ConnectorMapper connect(C config);

    /**
     * 断开连接
     *
     * @param connectorMapper
     */
    void disconnect(M connectorMapper);

    /**
     * 检查连接器是否连接正常
     *
     * @param connectorMapper
     * @return
     */
    boolean isAlive(M connectorMapper);

    /**
     * 获取连接缓存key
     *
     * @param config
     * @return
     */
    String getConnectorMapperCacheKey(C config);

    /**
     * 获取所有表名
     *
     * @param connectorMapper
     * @return
     */
    List<Table> getTable(M connectorMapper);

    /**
     * 获取表元信息
     *
     * @param connectorMapper
     * @param tableNamePattern
     * @return
     */
    MetaInfo getMetaInfo(M connectorMapper, String tableNamePattern);

    /**
     * 获取总数
     *
     * @param connectorMapper
     * @param command
     * @return
     */
    long getCount(M connectorMapper, Map<String, String> command);

    /**
     * 分页获取数据源数据
     *
     * @param config
     * @return
     */
    Result reader(M connectorMapper, ReaderConfig config);

    /**
     * 批量写入目标源数据
     *
     * @param config
     * @return
     */
    Result writer(M connectorMapper, WriterBatchConfig config);

    /**
     * 执行DDL命令
     *
     * @param connectorMapper
     * @param ddlConfig
     * @return
     */
    default Result writerDDL(M connectorMapper, DDLConfig ddlConfig){
        throw new ConnectorException("Unsupported method.");
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