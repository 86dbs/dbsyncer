package org.dbsyncer.parser.flush.impl;

import java.util.Date;
import java.util.LinkedList;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.config.GeneralBufferConfig;
import org.dbsyncer.common.event.RefreshOffsetEvent;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.model.IncrementConvertContext;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.ParserFactory;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.BatchWriter;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.parser.model.WriterResponse;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * 通用执行器（单线程消费，多线程批量写，按序执行）
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
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
    private Parser parser;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private CacheService cacheService;

    @Autowired
    private StorageService storageService;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private DDLParser ddlParser;

    @PostConstruct
    public void init() {
        setConfig(generalBufferConfig);
        buildConfig();
    }

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableGroupId();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        if (!CollectionUtils.isEmpty(request.getRow())) {
            response.getDataList().add(request.getRow());
        }
        response.getOffsetList().add(request.getChangedOffset());
        if (!response.isMerged()) {
            response.setTableGroupId(request.getTableGroupId());
            response.setEvent(request.getEvent());
            response.setSql(request.getSql());
            response.setMerged(true);
        }
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 并发场景，同一条数据可能连续触发Insert > Delete > Insert，批处理任务中出现不同事件时，跳过分区处理
        // 跳过表结构修改事件（保证表结构修改原子性）
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent()) || isDDLEvent(response.getEvent());
    }

    @Override
    protected void pull(WriterResponse response) {
        // 0、获取配置信息
        final TableGroup tableGroup = cacheService.get(response.getTableGroupId(), TableGroup.class);
        final Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);
        final TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);

        // 1、ddl解析
        if (isDDLEvent(response.getEvent())) {
            parseDDl(response, mapping, group);
            return;
        }

        final String sourceTableName = group.getSourceTable().getName();
        final String targetTableName = group.getTargetTable().getName();
        final String event = response.getEvent();
        final Picker picker = new Picker(group.getFieldMapping());
        final List<Map> sourceDataList = response.getDataList();
        // 2、映射字段
        List<Map> targetDataList = picker.pickData(sourceDataList);

        // 3、参数转换
        ConvertUtil.convert(group.getConvert(), targetDataList);

        // 4、插件转换
        final ConnectorMapper sConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getSourceConnectorId()));
        final ConnectorMapper tConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId()));
        final IncrementConvertContext context = new IncrementConvertContext(sConnectorMapper, tConnectorMapper, sourceTableName, targetTableName, event, sourceDataList, targetDataList);
        pluginFactory.convert(group.getPlugin(), context);

        // 5、批量执行同步
        BatchWriter batchWriter = new BatchWriter(tConnectorMapper, group.getCommand(), targetTableName, event, picker.getTargetFields(), targetDataList, generalBufferConfig.getBufferWriterCount());
        Result result = parser.writeBatch(context, batchWriter, generalExecutor);

        // 6.发布刷新增量点事件
        applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getOffsetList()));

        // 7、持久化同步结果
        result.setTableGroupId(tableGroup.getId());
        result.setTargetTableGroupName(targetTableName);
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, event);

        // 8、执行批量处理后的
        pluginFactory.postProcessAfter(group.getPlugin(), context);
    }

    @Override
    public Executor getExecutor() {
        return generalExecutor;
    }

    private boolean isDDLEvent(String event) {
        return StringUtil.equals(event, ConnectorConstant.OPERTION_ALTER);
    }

    /**
     * 解析DDL
     *
     * @param response
     * @param mapping
     * @param tableGroup
     */
    private void parseDDl(WriterResponse response, Mapping mapping, TableGroup tableGroup) {
        try {
            AbstractConnectorConfig sConnConfig = getConnectorConfig(mapping.getSourceConnectorId());
            AbstractConnectorConfig tConnConfig = getConnectorConfig(mapping.getTargetConnectorId());
            String sConnType = sConnConfig.getConnectorType();
            String tConnType = tConnConfig.getConnectorType();
            // 0.生成目标表执行SQL(暂支持MySQL) fixme AE86 暂内测MySQL作为试运行版本
            if (StringUtil.equals(sConnType, tConnType) && StringUtil.equals(ConnectorEnum.MYSQL.getType(), tConnType)) {
                String targetTableName = tableGroup.getTargetTable().getName();
                DDLConfig targetDDLConfig = ddlParser.parseDDlConfig(response.getSql(), tConnType, targetTableName,tableGroup.getFieldMapping());
                final ConnectorMapper tConnectorMapper = connectorFactory.connect(tConnConfig);
                Result result = connectorFactory.writerDDL(tConnectorMapper, targetDDLConfig);
                result.setTableGroupId(tableGroup.getId());
                result.setTargetTableGroupName(targetTableName);

                // TODO life
                // 1.获取目标表最新的属性字段
                MetaInfo tagertMetaInfo = parser.getMetaInfo(mapping.getTargetConnectorId(), targetTableName);
                MetaInfo originMetaInfo = parser.getMetaInfo(mapping.getSourceConnectorId(),tableGroup.getSourceTable().getName());
                // 1.1 参考 org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker.refreshTableFields
                //上面已经是刷新了

                // 1.2 要注意，表支持自定义主键，要兼容处理
                //主键问题还未涉及到这种情况，可能还没写，可能要是删除主键会需要考虑，其他的情况我应该不会动

                // 2.更新TableGroup.targetTable
                tableGroup.getSourceTable().setColumn(originMetaInfo.getColumn());
                tableGroup.getTargetTable().setColumn(tagertMetaInfo.getColumn());

                // 3.更新表字段映射(根据保留的更改的属性，进行更改)
                List<FieldMapping> fieldMappingList = tableGroup.getFieldMapping(); // 获得刚开始filedMapping
                List<FieldMapping> targetMappingList = new LinkedList<>();//替换完成后的filedMapping
                for (FieldMapping fieldMapping:fieldMappingList) {
                    String fieldSourceName = fieldMapping.getSource().getName();
                    String filedTargetName = fieldMapping.getTarget().getName();
                    //找到更改的源表的名称，也就是找到了对应的映射关系，这样就可以从源表找到更改后的名称进行对应，
                    if (fieldSourceName.equals(targetDDLConfig.getSourceColumnName())){
                        //找到源表的字段
                        if (targetDDLConfig.getSourceColumnName().equals(targetDDLConfig.getChangedColumnName())){//说明字段名没有改变，只是改变了属性
                            Field source = originMetaInfo.getColumn().stream()
                                    .filter(x->x.getName().equals(fieldSourceName)).findFirst().get();
                            Field target = tagertMetaInfo.getColumn().stream()
                                    .filter(x->x.getName().equals(filedTargetName)).findFirst().get();
                            //替换
                            targetMappingList.add(new FieldMapping(source,target)) ;
                            continue;
                        }
                    }
                    targetMappingList.add(fieldMapping);
                }
                tableGroup.setFieldMapping(targetMappingList);
                // 4.合并驱动配置 & 更新TableGroup.command 合并驱动应该不需要了，我只是把该替换的地方替换掉了，原来的还是保持一致，应该需要更新TableGroup.command
                // 4.1 参考 org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker.mergeConfig
                Map<String, String>  commands =  parser.getCommand(mapping,tableGroup);
                tableGroup.setCommand(commands);
                // 5.持久化存储 & 更新缓存
                // 5.1 参考 org.dbsyncer.manager.ManagerFactory.editConfigModel
                // 将方法移动到parser模块，就可以复用实现
                flushCache(tableGroup);
                applicationContext.publishEvent(new RefreshOffsetEvent(applicationContext, response.getOffsetList()));
                flushStrategy.flushIncrementData(mapping.getMetaId(), result, response.getEvent());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取连接器配置
     *
     * @param connectorId
     * @return
     */
    private AbstractConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }

    public void setGeneralExecutor(Executor generalExecutor) {
        this.generalExecutor = generalExecutor;
    }

    public void flushCache(TableGroup tableGroup){
        // 1、解析配置
        ConfigModel model = tableGroup;
        model.setCreateTime(new Date().getTime());
        model.setUpdateTime(new Date().getTime());
        Assert.notNull(model, "ConfigModel can not be null.");

        // 2、持久化
        Map<String, Object> params = ConfigModelUtil.convertModelToMap(model);
        logger.debug("params:{}", params);
        storageService.edit(StorageEnum.CONFIG, params);

        // 3、缓存
        // 1、缓存
        Assert.notNull(model, "ConfigModel can not be null.");
        String id = model.getId();
        cacheService.put(id, model);

        // 2、分组
//        String groupId = tableGroup.getId();
//        cacheService.putIfAbsent(groupId, new Group());
//        Group group = cacheService.get(groupId, Group.class);
//        group.addIfAbsent(id);
//        logger.debug("Put the model [{}] for {} group into cache.", id, groupId);
    }

}