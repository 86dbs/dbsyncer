package org.dbsyncer.parser;

import org.dbsyncer.common.event.ChangedEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.spi.ConvertContext;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.BatchWriter;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.Task;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

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
    ConnectorMapper connect(AbstractConnectorConfig config);

    /**
     * 刷新连接配置
     *
     * @param config
     */
    boolean refreshConnectorConfig(AbstractConnectorConfig config);

    /**
     * 连接配置是否可用
     *
     * @param config
     * @return
     */
    boolean isAliveConnectorConfig(AbstractConnectorConfig config);

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
     * @param executor
     */
    void execute(Task task, Mapping mapping, TableGroup tableGroup, Executor executor);

    /**
     * 增量同步
     *
     * @param tableGroupId 表关系ID
     * @param changedEvent 增量事件
     */
    void execute(String tableGroupId, ChangedEvent changedEvent);

    /**
     * 批执行
     *
     * @param context
     * @param batchWriter
     * @param executor
     * @return
     */
    Result writeBatch(ConvertContext context, BatchWriter batchWriter, Executor executor);

}