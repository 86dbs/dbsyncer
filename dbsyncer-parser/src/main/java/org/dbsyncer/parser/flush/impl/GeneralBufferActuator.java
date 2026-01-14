/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.QueueOverflowException;
import org.dbsyncer.common.config.GeneralBufferConfig;
import org.dbsyncer.common.metric.TimeRegistry;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.plugin.impl.IncrementPluginContext;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * 通用执行器（单线程消费，多线程批量写，按序执行）
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2022-03-27 16:50
 */
@Component
public class GeneralBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private GeneralBufferConfig generalBufferConfig;

    @Resource
    private Executor generalExecutor;

    @Resource
    private ConnectorFactory connectorFactory;


    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private DDLParser ddlParser;

    @Resource
    private TableGroupContext tableGroupContext;

    @Resource
    private LogService logService;

    @PostConstruct
    public void init() {
        setConfig(generalBufferConfig);
        buildConfig();
    }

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableName();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        if (!CollectionUtils.isEmpty(request.getRow())) {
            response.addData(request.getRow());
        }
        // 关键修复：只在第一次设置 ChangedOffset，后续不再覆盖
        // 问题：批次合并时，每次都会用当前请求的 ChangedOffset 覆盖 response 的 ChangedOffset
        //      导致批次使用的是最后一个事件的位置，而不是第一个事件的位置
        //      如果批次2先处理完成，快照更新到事件2000的位置，但批次1还没处理，停止后会丢失批次1
        // 解决：只在第一次设置 ChangedOffset（第一个事件的位置），后续不再覆盖
        //      这样批次使用的是第一个事件的位置，确保不会跳过队列中未处理的事件
        if (request.getChangedOffset() != null && response.getChangedOffset() == null) {
            response.setChangedOffset(request.getChangedOffset());
        }
        if (!response.isMerged()) {
            response.setTraceId(request.getTraceId());
            response.setTableName(request.getTableName());
            response.setEvent(request.getEvent());
            response.setTypeEnum(request.getTypeEnum());
            response.setSql(request.getSql());
            // 传递列名信息（从第一个请求中获取）
            response.setColumnNames(request.getColumnNames());
            response.setMerged(true);
        } else if (profileComponent.getSystemConfig().isEnablePrintTraceInfo() && StringUtil.isNotBlank(request.getTraceId())) {
            logger.info("traceId:{} merge into traceId:{}", request.getTraceId(), response.getTraceId());
        }
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 并发场景，同一条数据可能连续触发Insert > Delete > Insert，批处理任务中出现不同事件时，跳过分区处理
        // 跳过表结构修改事件（保证表结构修改原子性）
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent()) || ChangedEventTypeEnum.isDDL(response.getTypeEnum());
    }

    @Override
    public void pull(WriterResponse response) {
        // 所有任务事件处理完成后，由 process 方法统一刷新偏移量
        Meta meta = profileComponent.getMeta(response.getChangedOffset().getMetaId());
        assert meta != null;
        // 打印trace信息
        printTraceInfo(response);
        final Mapping mapping = profileComponent.getMapping(meta.getMappingId());
        List<TableGroupPicker> pickers = tableGroupContext.getTableGroupPickers(meta.getId(), response.getTableName());

        switch (response.getTypeEnum()) {
            case DDL:
                if (!mapping.getListener().isEnableDDL()) {
                    // DDL 被禁用，直接返回
                    break;
                }
                // DDL 处理：更新 fieldMapping 并强制刷新缓存
                // 1. 先处理 DDL，更新 TableGroup 的 fieldMapping
                List<TableGroup> updatedTableGroups = pickers.stream().map(picker -> {
                    TableGroup tableGroup = null;
                    try {
                        tableGroup = profileComponent.getTableGroup(picker.getTableGroup().getId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    parseDDl(response, mapping, tableGroup);
                    return tableGroup;
                }).collect(Collectors.toList());

                // 2. 强制刷新缓存，确保后续的 DML 事件使用最新的 fieldMapping
                // 注意：这里使用更新后的 TableGroup，确保 fieldMapping 已同步
                tableGroupContext.update(mapping, updatedTableGroups);

                // 3. 验证缓存已更新（可选，用于调试）
                if (logger.isDebugEnabled()) {
                    List<TableGroupPicker> refreshedPickers = tableGroupContext.getTableGroupPickers(meta.getId(), response.getTableName());
                    logger.debug("DDL 处理完成，已刷新 TableGroupPicker 缓存。表名: {}, 缓存数量: {}",
                            response.getTableName(), refreshedPickers.size());
                }
                break;
            case SCAN:
                pickers.forEach(picker -> {
                    distributeTableGroup(response, mapping, picker, picker.getSourceFields(), false);
                });
                break;
            case ROW:
                pickers.forEach(picker -> {
                    distributeTableGroup(response, mapping, picker, picker.getSourceFields(), true);
                });
                break;
        }
    }

    @Override
    protected void offerFailed(BlockingQueue<WriterRequest> queue, WriterRequest request) {
        throw new QueueOverflowException("缓存队列已满");
    }

    @Override
    protected void meter(TimeRegistry timeRegistry, long count) {
        // 统计执行器同步效率TPS
        timeRegistry.meter(TimeRegistry.GENERAL_BUFFER_ACTUATOR_TPS).add(count);
    }

    @Override
    public Executor getExecutor() {
        return generalExecutor;
    }

    private void distributeTableGroup(WriterResponse response, Mapping mapping, TableGroupPicker tableGroupPicker, List<Field> sourceFields, boolean enableFilter) {
        if (response.getColumnNames() == null || response.getColumnNames().isEmpty()) {
            return;
        }

        Result result;
        TableGroup tableGroup = tableGroupPicker.getTableGroup();

        // 1、映射字段
        // 优先使用事件携带的列名信息，确保字段映射与数据一致
        List<Field> actualSourceFields;
        // 根据列名从 TableGroup 中查找对应的 Field 信息
        actualSourceFields = buildFieldsFromColumnNames(response.getColumnNames(), sourceFields);
        // 如果无法构建完整的字段列表，回退到使用传入的 sourceFields
        if (actualSourceFields.size() != sourceFields.size()) {
            String msg = String.format("字段定位错误！mappingId: %s, 表名: %s, 列名: %s",
                    mapping.getId(), response.getTableName(), response.getColumnNames());
            logger.error(msg);
            logService.log(LogType.MappingLog.RUNNING, msg);
            result = new Result();
            result.error = msg;
            result.setTableGroupId(tableGroup.getId());
            result.setTargetTableGroupName(tableGroup.getTargetTable().getName());
            flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());
            return;
        }

        boolean enableSchemaResolver = profileComponent.getSystemConfig().isEnableSchemaResolver();
        ConnectorConfig sourceConfig = getConnectorConfig(mapping.getSourceConnectorId());
        ConnectorService sourceConnector = connectorFactory.getConnectorService(sourceConfig.getConnectorType());
        List<Map> sourceDataList = new ArrayList<>();
        List<Map> targetDataList = tableGroupPicker.getPicker()
                .setSourceResolver(enableSchemaResolver ? sourceConnector.getSchemaResolver() : null)
                .pickTargetData(actualSourceFields, enableFilter, response.getDataList(), sourceDataList);
        if (CollectionUtils.isEmpty(targetDataList)) {
            return;
        }

        // 2、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), targetDataList);

        // 3、插件转换
        final IncrementPluginContext context = new IncrementPluginContext();
        try {
            context.setSourceConnectorInstance(connectorFactory.connect(sourceConfig));
            context.setTargetConnectorInstance(connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId())));
        } catch (Exception e) {
            String msg = String.format("获取连接异常！mappingId: %s，msg %s",
                    mapping.getId(), e.getMessage());
            logger.error(msg);
            logService.log(LogType.MappingLog.RUNNING, msg);
            result = new Result();
            result.error = msg;
            result.setTableGroupId(tableGroup.getId());
            result.setTargetTableGroupName(tableGroup.getTargetTable().getName());
            flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());
            return;
        }
        context.setSourceTableName(tableGroup.getSourceTable().getName());
        context.setTargetTableName(tableGroup.getTargetTable().getName());
        context.setTraceId(response.getTraceId());
        context.setEvent(response.getEvent());
        List<Field> targetFields = tableGroupPicker.getTargetFields();
        Assert.isTrue(targetFields != null && !targetFields.isEmpty(), "targetFields can not be empty");
        context.setTargetFields(targetFields);
        context.setCommand(tableGroup.getCommand());
        context.setBatchSize(getBufferWriterCount());
        context.setSourceList(sourceDataList);
        context.setTargetList(targetDataList);
        context.setPlugin(tableGroup.getPlugin());
        context.setPluginExtInfo(tableGroup.getPluginExtInfo());
        context.setForceUpdate(mapping.isForceUpdate());
        context.setEnableSchemaResolver(enableSchemaResolver);
        context.setEnablePrintTraceInfo(StringUtil.isNotBlank(response.getTraceId()));
        // 设置Mapping参数
        if (mapping.getParams() != null) {
            context.getCommand().putAll(mapping.getParams());
        }
        try {
            pluginFactory.process(context, ProcessEnum.CONVERT);

            // 4、批量执行同步
            result = connectorFactory.writeBatch(context);

            // 5、持久化同步结果
            result.setTableGroupId(tableGroup.getId());
            result.setTargetTableGroupName(context.getTargetTableName());

            // 6、执行后置处理
            pluginFactory.process(context, ProcessEnum.AFTER);
        } catch (Exception e) {
            logger.error("process batch data error:", e);
            result = new Result();
            result.error = e.getMessage();
            result.setTableGroupId(tableGroup.getId());
            result.setTargetTableGroupName(context.getTargetTableName());
            result.addFailData(targetDataList);
        }
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());
    }

    /**
     * 解析DDL
     * 完整的DDL处理流程：解析DDL → 执行DDL（如果启用）→ 刷新表结构 → 更新字段映射
     *
     * @param response   WriterResponse，包含DDL SQL和事件信息
     * @param mapping    Mapping配置，包含源和目标连接器ID
     * @param tableGroup TableGroup配置，包含表结构和字段映射
     */
    public void parseDDl(WriterResponse response, Mapping mapping, TableGroup tableGroup) {
        try {
            logger.info("开始处理 DDL: mapping={}, table={}, sql={}", mapping.getName(), tableGroup.getTargetTable().getName(), response.getSql());

            ListenerConfig listenerConfig = mapping.getListener();

            // 注意：全局 DDL 开关检查已在 Listener 和 pull() 方法中完成，此处无需重复检查

            // 1. 解析 DDL 获取操作类型
            ConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
            String tConnType = tConnConfig.getConnectorType();
            ConnectorService connectorService = connectorFactory.getConnectorService(tConnType);
            // 传递源和目标连接器类型信息给DDL解析器
            DDLConfig targetDDLConfig = ddlParser.parse(connectorService, tableGroup, response.getSql());
            logger.info("DDL 解析完成: 操作类型={}, 目标SQL={}", targetDDLConfig.getDdlOperationEnum(), targetDDLConfig.getSql());

            // 3. 根据操作类型检查细粒度配置
            DDLOperationEnum operation = targetDDLConfig.getDdlOperationEnum();
            if (!isDDLOperationAllowed(listenerConfig, operation)) {
                logger.warn("DDL 操作被配置禁用，跳过执行。操作类型: {}, 表: {}",
                        operation, tableGroup.getTargetTable().getName());
                // 注意：如果 DDL 被禁用，应该提前返回，不执行后续的字段映射更新
                // 这样可以避免字段映射与实际表结构不一致的问题
                // refreshOffset 会在外部统一调用
                return;
            }

            // 4. 生成目标表执行SQL(支持异构数据库)
            logger.info("准备执行目标 DDL: table={}, sql={}", tableGroup.getTargetTable().getName(), targetDDLConfig.getSql());
            ConnectorInstance tConnectorInstance = connectorFactory.connect(tConnConfig);
            Result result = connectorFactory.writerDDL(tConnectorInstance, targetDDLConfig);
            if (StringUtil.isBlank(result.error)) {
                logger.info("目标 DDL 执行成功: table={}", tableGroup.getTargetTable().getName());
            } else {
                logger.error("目标 DDL 执行失败: table={}, error={}", tableGroup.getTargetTable().getName(), result.error);
            }
            // 5.持久化增量事件数据(含错误信息)
            result.setTableGroupId(tableGroup.getId());
            result.setTargetTableGroupName(tableGroup.getTargetTable().getName());
            flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());

            // 6. 如果 DDL 执行失败，提前返回，不更新字段映射
            // refreshOffset 会在外部统一调用
            if (!StringUtil.isBlank(result.error)) {
                return;
            }

            // 7.更新表属性字段（DDL 执行成功后）
            MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(connectorFactory.connect(getConnectorConfig(mapping.getSourceConnectorId())), tableGroup.getSourceTable().getName());
            MetaInfo targetMetaInfo = connectorFactory.getMetaInfo(connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId())), tableGroup.getTargetTable().getName());
            tableGroup.getSourceTable().setColumn(sourceMetaInfo.getColumn());
            tableGroup.getTargetTable().setColumn(targetMetaInfo.getColumn());

            // 8.更新表字段映射关系
            ddlParser.refreshFiledMappings(tableGroup, targetDDLConfig);
            logger.info("字段映射关系已更新: table={}, 映射数量={}", tableGroup.getTargetTable().getName(),
                    tableGroup.getFieldMapping() != null ? tableGroup.getFieldMapping().size() : 0);

            // 9.更新执行命令
            tableGroup.initCommand(mapping, connectorFactory);

            // 10.持久化存储 & 更新缓存配置
            profileComponent.editTableGroup(tableGroup);
            logger.info("TableGroup 已持久化: table={}", tableGroup.getTargetTable().getName());

            logger.info("DDL 处理完成: mapping={}, table={}", mapping.getName(), tableGroup.getTargetTable().getName());
        } catch (Exception e) {
            logger.error("DDL 处理异常: mapping={}, table={}, error={}", mapping.getName(),
                    tableGroup.getTargetTable().getName(), e.getMessage(), e);
            // refreshOffset 会在外部统一调用，这里不需要处理
        }
    }

    /**
     * 检查 DDL 操作是否被允许
     *
     * @param config    监听器配置
     * @param operation DDL 操作类型
     * @return 是否允许执行
     */
    private boolean isDDLOperationAllowed(ListenerConfig config, DDLOperationEnum operation) {
        switch (operation) {
            case ALTER_ADD:
                return config.isAllowAddColumn();
            case ALTER_DROP:
                return config.isAllowDropColumn();
            case ALTER_MODIFY:
            case ALTER_CHANGE:
                return config.isAllowModifyColumn();
            default:
                logger.warn("未知的 DDL 操作类型: {}", operation);
                return false;
        }
    }

    /**
     * 根据列名列表构建字段列表
     * 优先使用事件自带的列名顺序，确保与 CDC 数据顺序一致
     *
     * @param columnNames  CDC 事件中的列名列表（按数据顺序）
     * @param tableColumns TableGroup 中的源表字段列表
     * @return 按 columnNames 顺序排列的字段列表（去重，每个字段只出现一次）
     */
    private List<Field> buildFieldsFromColumnNames(List<String> columnNames, List<Field> tableColumns) {
        if (CollectionUtils.isEmpty(columnNames) || CollectionUtils.isEmpty(tableColumns)) {
            return new ArrayList<>();
        }

        // 构建字段名到字段对象的映射
        Map<String, Field> fieldMap = tableColumns.stream()
                .collect(Collectors.toMap(Field::getName, field -> field, (k1, k2) -> k1));

        // 按照 columnNames 的顺序构建字段列表
        // 使用 LinkedHashSet 保持顺序并去重，防止重复的列名导致同一个字段被添加多次
        List<Field> fields = new ArrayList<>();
        Set<String> addedFieldNames = new LinkedHashSet<>();
        for (String columnName : columnNames) {
            Field field = fieldMap.get(columnName);
            if (field != null) {
                // 只有当字段名还没有被添加过时，才添加到列表中
                // 这样可以防止 columnNames 中有重复列名时，同一个字段被添加多次
                if (addedFieldNames.add(columnName)) {
                    fields.add(field);
                }
                if (fields.size() == tableColumns.size()) {
                    return fields;
                }
            } else {
                // 如果找不到对应的字段，记录警告但继续处理
                logger.warn("事件中的列名 '{}' 在 TableGroup 中未找到对应的字段信息", columnName);
            }
        }

        return fields;
    }

    /**
     * 获取连接器配置
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = profileComponent.getConnector(connectorId);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }

    private void printTraceInfo(WriterResponse response) {
        if (profileComponent.getSystemConfig().isEnablePrintTraceInfo() && StringUtil.isNotBlank(response.getTraceId())) {
            logger.info("traceId:{}, tableName:{}, event:{}, offset:{}, row:{}", response.getTraceId(), response.getTableName(), response.getEvent(), JsonUtil.objToJson(response.getChangedOffset()), response.getDataList());
        }
    }
}