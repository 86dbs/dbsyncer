package org.dbsyncer.parser.flush.impl;

import org.apache.commons.logging.Log;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.config.BufferActuatorConfig;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.parser.ParserFactory;
import org.dbsyncer.parser.flush.AbstractBufferActuator;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.strategy.ParserStrategy;
import org.dbsyncer.parser.util.ConvertUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:50
 */
@Component
public class WriterBufferActuator extends AbstractBufferActuator<WriterRequest, WriterResponse> {

    private final Logger logger = LoggerFactory.getLogger(getClass());


    @Autowired
    private ConnectorFactory connectorFactory;

    @Autowired
    private ParserFactory parserFactory;

    @Autowired
    private PluginFactory pluginFactory;

    @Autowired
    private FlushStrategy flushStrategy;

    @Autowired
    private ParserStrategy parserStrategy;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private BufferActuatorConfig bufferActuatorConfig;

    @Override
    protected String getPartitionKey(WriterRequest request) {
        return request.getTableGroupId();
    }

    @Override
    protected void partition(WriterRequest request, WriterResponse response) {
        response.getDataList().add(request.getRow());
        if(StringUtil.isNotBlank(request.getMessageId())){
            response.getMessageIds().add(request.getMessageId());
        }
        if (response.isMerged()) {
            return;
        }
        response.setTableGroupId(request.getTableGroupId());
        response.setEvent(request.getEvent());
        response.setMerged(true);
    }

    @Override
    protected void pull(WriterResponse response) {
        // 1、获取配置信息
        final TableGroup tableGroup = cacheService.get(response.getTableGroupId(), TableGroup.class);
        final Mapping mapping = cacheService.get(tableGroup.getMappingId(), Mapping.class);
        final String targetTableName = tableGroup.getTargetTable().getName();
        final String event = response.getEvent();
        final List<Map> sourceDataList = response.getDataList();

        // 2、映射字段
        final Picker picker = new Picker(tableGroup.getFieldMapping());
        List<Map> targetDataList = picker.pickData(sourceDataList);

        // 3、参数转换
        ConvertUtil.convert(tableGroup.getConvert(), targetDataList);

        // 4、插件转换
        pluginFactory.convert(tableGroup.getPlugin(), event, sourceDataList, targetDataList);
        // 5、批量执行同步
        ConnectorMapper targetConnectorMapper = connectorFactory.connect(getConnectorConfig(mapping.getTargetConnectorId()));
        BatchWriter batchWriter = new BatchWriter(targetConnectorMapper, tableGroup.getCommand(), targetTableName, event, picker.getTargetFields(), targetDataList, bufferActuatorConfig.getWriterBatchCount());
        Result result = parserFactory.writeBatch(batchWriter);


        // 6、持久化同步结果
        flushStrategy.flushIncrementData(mapping.getMetaId(), result, event);

        //6.2、执行批量处理后的
        //by wangxir @20221025
        try {
            if (targetConnectorMapper.getConnection() instanceof Connection)
                pluginFactory.AfterConvert((Connection) targetConnectorMapper.getConnection(),targetTableName,mapping.getPlugin(), event, sourceDataList, targetDataList);

        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage());
        }

        // 7、完成处理
        parserStrategy.complete(response.getMessageIds());
    }

    @Override
    protected boolean skipPartition(WriterRequest nextRequest, WriterResponse response) {
        // 并发场景，同一条数据可能连续触发Insert > Delete > Insert，批处理任务中出现不同事件时，跳过分区处理
        return !StringUtil.equals(nextRequest.getEvent(), response.getEvent());
    }

    /**
     * 获取连接器配置
     *
     * @param connectorId
     * @return
     */
    private ConnectorConfig getConnectorConfig(String connectorId) {
        Assert.hasText(connectorId, "Connector id can not be empty.");
        Connector conn = cacheService.get(connectorId, Connector.class);
        Assert.notNull(conn, "Connector can not be null.");
        return conn.getConfig();
    }
}