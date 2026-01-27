/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.apache.lucene.index.IndexableField;
import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.BinlogColumnVo;
import org.dbsyncer.biz.vo.DataVo;
import org.dbsyncer.biz.vo.MessageVo;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.*;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.flush.impl.MetaBufferActuator;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.FieldResolver;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 数据同步服务
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/12/19 23:56
 */
@Service
public class DataSyncServiceImpl implements DataSyncService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private BufferActuatorRouter bufferActuatorRouter;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private StorageService storageService;

    @Resource
    private MonitorService monitorService;

    @Override
    public MessageVo getMessageVo(String metaId, String messageId) throws Exception {
        Assert.hasText(metaId, "The metaId is null.");
        Assert.hasText(messageId, "The messageId is null.");

        MessageVo messageVo = new MessageVo();
        Map row = getData(metaId, messageId);
        Map binlogData = getBinlogData(row, true);
        String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
        if (tableGroup == null) {
            String msg = String.format("table group id: %s,  is not exist. meta id: %s, ", tableGroupId, metaId);
            throw new RuntimeException(msg);
        }
        messageVo.setSourceTableName(tableGroup.getSourceTable().getName());
        messageVo.setTargetTableName(tableGroup.getTargetTable().getName());
        messageVo.setId(messageId);

        if (!CollectionUtils.isEmpty(binlogData)) {
            Map<String, String> columnMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, Field::getTypeName));
            List<BinlogColumnVo> columns = new ArrayList<>();
            binlogData.forEach((k, v) -> columns.add(new BinlogColumnVo((String) k, v, columnMap.get(k))));
            messageVo.setColumns(columns);
        }
        return messageVo;
    }

    @Override
    public Map getBinlogData(Map row, boolean prettyBytes) throws Exception {
        String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
        // 1、获取配置信息
        final TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
        if (tableGroup == null) {
            return Collections.EMPTY_MAP;
        }

        // 2、获取记录的数据
        byte[] bytes = (byte[]) row.get(ConfigConstant.BINLOG_DATA);
        if (null == bytes) {
            if (prettyBytes) {
                String json = (String) row.get(ConfigConstant.CONFIG_MODEL_JSON);
                return JsonUtil.parseMap(json);
            }
            return Collections.EMPTY_MAP;
        }

        // 3、获取DDL
        Map<String, Object> target = new HashMap<>();
        BinlogMap message = BinlogMap.parseFrom(bytes);
        String event = (String) row.get(ConfigConstant.DATA_EVENT);
        if (StringUtil.equals(event, ConnectorConstant.OPERTION_ALTER)) {
            // DDL事件：从BINLOG_DATA键中获取DDLConfig的JSON字符串
            message.getRowMap().forEach((k, v) -> {
                if (ConfigConstant.BINLOG_DATA.equals(k)) {
                    // 保存完整的DDLConfig JSON字符串，以便重做时反序列化
                    target.put(k, v.toStringUtf8());
                } else {
                    target.put(k, v.toStringUtf8());
                }
            });
            return target;
        }

        // 4、反序列化
        // 注意：存储的是源数据（sourceDataList），字段名是源表字段名
        // 所以需要使用源表字段类型来反序列化，直接使用源数据
        final Map<String, Field> sourceFieldMap = tableGroup.getSourceTable().getColumn().stream()
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toMap(Field::getName, f -> f, (k1, k2) -> k1));
        message.getRowMap().forEach((k, v) -> {
            if (sourceFieldMap.containsKey(k)) {
                Object val = BinlogMessageUtil.deserializeValue(sourceFieldMap.get(k).getType(), v);
                // 处理二进制对象显示
                if (prettyBytes) {
                    if (val instanceof byte[]) {
                        byte[] b = (byte[]) val;
                        if (b.length > 128) {
                            target.put(k, String.format("byte[%d]", b.length));
                            return;
                        }
                        target.put(k, Arrays.toString(b));
                        return;
                    }
                }
                target.put(k, val);
            }
        });
        return target;
    }

    @Override
    public String sync(Map<String, String> params) throws Exception {
        String metaId = params.get("metaId");
        String messageId = params.get("messageId");
        Assert.hasText(metaId, "The metaId is null.");
        Assert.hasText(messageId, "The messageId is null.");

        Map row = getData(metaId, messageId);
        if (CollectionUtils.isEmpty(row)) {
            logger.warn("重试失败：无法从存储中获取数据, metaId={}, messageId={}", metaId, messageId);
            return messageId;
        }

        Map binlogData = getBinlogData(row, false);
        if (CollectionUtils.isEmpty(binlogData)) {
            logger.warn("重试失败：无法解析binlog数据, metaId={}, messageId={}", metaId, messageId);
            return messageId;
        }

        String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
        if (StringUtil.isBlank(tableGroupId)) {
            logger.warn("重试失败：tableGroupId为空, metaId={}, messageId={}", metaId, messageId);
            return messageId;
        }

        String event = (String) row.get(ConfigConstant.DATA_EVENT);
        if (StringUtil.isBlank(event)) {
            logger.warn("重试失败：event为空, metaId={}, messageId={}", metaId, messageId);
            return messageId;
        }

        TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
        if (tableGroup == null) {
            logger.warn("重试失败：TableGroup不存在, tableGroupId={}, metaId={}, messageId={}", tableGroupId, metaId, messageId);
            return messageId;
        }

        if (tableGroup.getSourceTable() == null) {
            logger.warn("重试失败：TableGroup的源表为空, tableGroupId={}, metaId={}, messageId={}", tableGroupId, metaId, messageId);
            return messageId;
        }

        String sourceTableName = tableGroup.getSourceTable().getName();

        // 判断是否为DDL事件
        if (StringUtil.equals(event, ConnectorConstant.OPERTION_ALTER)) {
            // DDL重做：从binlogData中获取DDLConfig的JSON字符串并反序列化
            String ddlConfigJson = (String) binlogData.get(ConfigConstant.BINLOG_DATA);
            if (StringUtil.isBlank(ddlConfigJson)) {
                logger.warn("DDL重做失败：无法从存储中获取DDLConfig数据, messageId={}", messageId);
                return messageId;
            }

            try {
                // 反序列化为DDLConfig对象
                DDLConfig ddlConfig = JsonUtil.jsonToObj(ddlConfigJson, DDLConfig.class);
                if (ddlConfig == null || StringUtil.isBlank(ddlConfig.getSql())) {
                    logger.warn("DDL重做失败：DDLConfig数据无效, messageId={}", messageId);
                    return messageId;
                }

                // 创建DDLChangedEvent，通过BufferActuatorRouter执行，会自动调用parseDDl方法
                // parseDDl方法会执行DDL、更新表结构、更新字段映射、持久化配置
                DDLChangedEvent ddlChangedEvent = new DDLChangedEvent(
                        sourceTableName,
                        event,
                        ddlConfig.getSql(),
                        null,
                        null
                );

                // 设置ChangedOffset的metaId（changedOffset是final的，需要通过getChangedOffset()获取后设置）
                ddlChangedEvent.getChangedOffset().setMetaId(metaId);

                // 直接调用执行器的方法，不走队列
                MetaBufferActuator actuator = bufferActuatorRouter.getOrCreateActuator(metaId);
                actuator.executeDirectly(ddlChangedEvent);

            } catch (Exception e) {
                logger.error("DDL重做失败, messageId={}, error={}", messageId, e.getMessage(), e);
                // 执行失败，不删除数据，保留以便后续重试
                return messageId;
            }
        } else {
            // 数据同步重做：处理ROW事件
            // 有修改同步值
            String retryDataParams = params.get("retryDataParams");
            if (StringUtil.isNotBlank(retryDataParams)) {
                JsonUtil.parseMap(retryDataParams).forEach((k, v) -> binlogData.put(k, convertValue(binlogData.get(k), (String) v)));
            }

            // 直接使用源数据，转换为List<Object>格式
            final Picker picker = new Picker(tableGroup);
            List<Field> sourceFields = picker.getSourceFields();
            List<Object> changedRow = new ArrayList<>();
            // 按照 sourceFields 的顺序构建 columnNames，确保与 changedRow 的顺序和数量一致
            List<String> columnNames = new ArrayList<>();
            for (Field field : sourceFields) {
                changedRow.add(binlogData.get(field.getName()));
                columnNames.add(field.getName());
            }

            // 创建RowChangedEvent时传入columnNames，确保重试时能正确处理
            RowChangedEvent changedEvent = new RowChangedEvent(sourceTableName, event, changedRow, null, null, columnNames);

            // 设置ChangedOffset的metaId（changedOffset是final的，需要通过getChangedOffset()获取后设置）
            changedEvent.getChangedOffset().setMetaId(metaId);

            try {
                // 直接执行数据重做（不走队列，同步执行）
                MetaBufferActuator actuator = bufferActuatorRouter.getOrCreateActuator(metaId);
                actuator.executeDirectly(changedEvent);

            } catch (Exception e) {
                logger.error("数据重做失败, messageId={}, error={}", messageId, e.getMessage(), e);
                // 执行失败，不删除数据，保留以便后续重试
                return messageId;
            }
        }

        // 执行成功后，删除存储中的失败数据并更新统计
        // 注意：如果 executeDirectly() 抛出异常，说明重试失败，不会执行到这里
        try {
            storageService.remove(StorageEnum.DATA, metaId, messageId);
            // 更新统计数据：减少失败数，增加成功数
            Meta meta = profileComponent.getMeta(metaId);
            if (meta != null) {
                meta.getFail().decrementAndGet();
                meta.getSuccess().incrementAndGet();
                meta.setUpdateTime(Instant.now().toEpochMilli());
                
                    // 减少失败数，增加成功数
                    tableGroup.setFail(Math.max(0, tableGroup.getFail() - 1));
                    tableGroup.setSuccess(tableGroup.getSuccess() + 1);
                    // 更新同步状态：根据失败计数判断（有失败记录则为"异常"，否则为"正常"）
                    tableGroup.setStatus(tableGroup.getFail() > 0 ? "异常" : "正常");
                    // 标记 TableGroup 计数发生变化
                    meta.markTableGroupChanged(tableGroupId);
                
                profileComponent.editConfigModel(meta);
            } else {
                logger.warn("重试成功但Meta不存在，无法更新统计, metaId={}, messageId={}", metaId, messageId);
            }
        } catch (Exception e) {
            // 删除数据或更新统计失败，记录日志但不影响重试结果
            logger.error("重试成功但清理数据或更新统计失败, metaId={}, messageId={}, error={}", metaId, messageId, e.getMessage(), e);
        }

        return messageId;
    }

    private Map getData(String metaId, String messageId) {
        Query query = new Query(1, 1);
        Map<String, FieldResolver> fieldResolvers = new ConcurrentHashMap<>();
        fieldResolvers.put(ConfigConstant.BINLOG_DATA, (FieldResolver<IndexableField>) field -> field.binaryValue().bytes);
        query.setFieldResolverMap(fieldResolvers);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, messageId);
        query.setMetaId(metaId);
        query.setType(StorageEnum.DATA);
        Paging paging = storageService.query(query);
        if (!CollectionUtils.isEmpty(paging.getData())) {
            List<Map> data = (List<Map>) paging.getData();
            return data.get(0);
        }
        return Collections.EMPTY_MAP;
    }

    private Object convertValue(Object oldValue, String newValue) {
        if (oldValue == null) {
            return newValue;
        }

        Object newVal;
        String type = oldValue.getClass().getName();
        switch (type) {
            case "java.sql.Date":
                newVal = DateFormatUtil.stringToDate(newValue);
                break;
            case "java.sql.Timestamp":
                newVal = DateFormatUtil.stringToTimestamp(newValue);
                break;
            case "java.lang.Integer":
            case "java.lang.Short":
                newVal = NumberUtil.toInt(newValue);
                break;
            case "java.lang.Long":
                newVal = NumberUtil.toLong(newValue);
                break;
            case "java.lang.Float":
                newVal = Float.valueOf(newValue);
                break;
            case "java.lang.Double":
                newVal = Double.valueOf(newValue);
                break;
            case "[B":
                newVal = stringToBytes(newValue);
                break;
            default:
                newVal = newValue;
        }

        return newVal;
    }

    private byte[] stringToBytes(String s) {
        byte[] b = null;
        if (s.startsWith("[") && s.endsWith("]")) {
            s = StringUtil.substring(s, 1, s.length() - 1);
            String[] split = StringUtil.split(s, StringUtil.COMMA);
            int length = split.length;
            b = new byte[length];
            for (int i = 0; i < length; i++) {
                b[i] = Byte.valueOf(split[i].trim());
            }
        }
        return b;
    }

    @Override
    public Map<String, Object> retryAll(String metaId) throws Exception {
        Assert.hasText(metaId, "The metaId is null.");

        int totalCount = 0;
        int successCount = 0;
        int failCount = 0;
        int pageSize = 100; // 每页查询100条
        int pageNum = 1;

        // 分页查询失败数据，直到没有更多数据
        while (true) {
            Map<String, String> params = new HashMap<>();
            params.put(ConfigConstant.CONFIG_MODEL_ID, metaId);
            params.put("dataStatus", "0"); // 失败状态
            params.put("pageNum", String.valueOf(pageNum));
            params.put("pageSize", String.valueOf(pageSize));

            Paging paging = monitorService.queryData(params);
            List<DataVo> dataList = (List<DataVo>) paging.getData();

            if (CollectionUtils.isEmpty(dataList)) {
                break; // 没有更多数据
            }

            // 逐个重试
            for (DataVo dataVo : dataList) {
                totalCount++;
                String messageId = dataVo.getId();
                if (StringUtil.isBlank(messageId)) {
                    logger.warn("重试失败：messageId为空, metaId={}", metaId);
                    failCount++;
                    continue;
                }

                try {
                    Map<String, String> syncParams = new HashMap<>();
                    syncParams.put("metaId", metaId);
                    syncParams.put("messageId", messageId);
                    String result = sync(syncParams);
                    
                    // 如果返回的messageId与传入的相同，说明重试失败（sync方法失败时返回原messageId）
                    if (messageId.equals(result)) {
                        failCount++;
                    } else {
                        successCount++;
                    }
                } catch (Exception e) {
                    logger.error("批量重试失败, metaId={}, messageId={}, error={}", metaId, messageId, e.getMessage(), e);
                    failCount++;
                }
            }

            // 如果当前页数据少于pageSize，说明已经是最后一页
            if (dataList.size() < pageSize) {
                break;
            }

            pageNum++;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("total", totalCount);
        result.put("success", successCount);
        result.put("fail", failCount);
        return result;
    }

}