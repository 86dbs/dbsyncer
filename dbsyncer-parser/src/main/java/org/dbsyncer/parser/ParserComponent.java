package org.dbsyncer.parser;

import org.dbsyncer.common.event.ChangedEvent;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.spi.ConvertContext;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.parser.model.BatchWriter;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.Task;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * <pre>
 *     实现全量和增量同步任务，以及连接器读写，插件转换，表元信息，心跳检测
 * </pre>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/20 21:11
 */
public interface ParserComponent {

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