package org.dbsyncer.parser;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/20 21:11
 */
public interface Parser {

    /**
     * 获取连接配置
     *
     * @param config
     * @return
     */
    ConnectorMapper connect(ConnectorConfig config);

    /**
     * 刷新连接配置
     *
     * @param config
     */
    boolean refreshConnectorConfig(ConnectorConfig config);

    /**
     * 连接配置是否可用
     *
     * @param config
     * @return
     */
    boolean isAliveConnectorConfig(ConnectorConfig config);

    /**
     * 获取连接器表
     *
     * @param config
     * @return
     */
    List<Table> getTable(ConnectorMapper config);

    /**
     * 获取表元信息
     *
     * @param connectorId
     * @param tableName
     * @return
     */
    MetaInfo getMetaInfo(String connectorId, String tableName);

    /**
     * 获取映射关系执行命令
     *
     * @param mapping
     * @param tableGroup
     * @return
     */
    Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup);

    /**
     * 获取总数
     *
     * @param connectorId
     * @param command
     * @return
     */
    long getCount(String connectorId, Map<String, String> command);

    /**
     * 解析连接器配置为Connector
     *
     * @param json
     * @return
     */
    Connector parseConnector(String json);

    /**
     * 解析配置
     *
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    <T> T parseObject(String json, Class<T> clazz);

    /**
     * 获取所有连接器类型
     *
     * @return
     */
    List<ConnectorEnum> getConnectorEnumAll();

    /**
     * 获取所有条件类型
     *
     * @return
     */
    List<OperationEnum> getOperationEnumAll();

    /**
     * 获取过滤条件系统参数
     *
     * @return
     */
    List<QuartzFilterEnum> getQuartzFilterEnumAll();

    /**
     * 获取所有运算符类型
     *
     * @return
     */
    List<FilterEnum> getFilterEnumAll();

    /**
     * 获取所有转换类型
     *
     * @return
     */
    List<ConvertEnum> getConvertEnumAll();

    /**
     * 获取所有同步数据状态类型
     *
     * @return
     */
    List<StorageDataStatusEnum> getStorageDataStatusEnumAll();

    /**
     * 全量同步
     *
     * @param task
     * @param mapping
     * @param tableGroup
     */
    void execute(Task task, Mapping mapping, TableGroup tableGroup);

    /**
     * 增量同步
     *
     * @param mapping
     * @param tableGroup
     * @param rowChangedEvent
     */
    void execute(Mapping mapping, TableGroup tableGroup, RowChangedEvent rowChangedEvent);

    /**
     * 批执行
     *
     * @param batchWriter
     * @return
     */
    Result writeBatch(BatchWriter batchWriter);

}