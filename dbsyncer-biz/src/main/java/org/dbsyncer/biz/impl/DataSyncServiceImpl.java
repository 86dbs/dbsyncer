/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.index.IndexableField;
import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.vo.BinlogColumnVo;
import org.dbsyncer.biz.vo.MessageVo;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.FieldResolver;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.config.DDLConfig;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Override
    public MessageVo getMessageVo(String metaId, String messageId) {
        Assert.hasText(metaId, "The metaId is null.");
        Assert.hasText(messageId, "The messageId is null.");

        MessageVo messageVo = new MessageVo();
        try {
            Map row = getData(metaId, messageId);
            Map binlogData = getBinlogData(row, true);
            String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
            TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
            messageVo.setSourceTableName(tableGroup.getSourceTable().getName());
            messageVo.setTargetTableName(tableGroup.getTargetTable().getName());
            messageVo.setId(messageId);

            if (!CollectionUtils.isEmpty(binlogData)) {
                Map<String, String> columnMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, Field::getTypeName));
                List<BinlogColumnVo> columns = new ArrayList<>();
                binlogData.forEach((k, v) -> columns.add(new BinlogColumnVo((String) k, v, columnMap.get(k))));
                messageVo.setColumns(columns);
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
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

        // 4、反序列
        final Picker picker = new Picker(tableGroup);
        final Map<String, Field> fieldMap = picker.getTargetFieldMap();
        message.getRowMap().forEach((k, v) -> {
            if (fieldMap.containsKey(k)) {
                try {
                    Object val = BinlogMessageUtil.deserializeValue(fieldMap.get(k).getType(), v);
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
                } catch (Exception e) {
                    logger.warn("解析Binlog数据类型异常：type=[{}], valueType=[{}], value=[{}]", fieldMap.get(k).getType(),
                            (v == null ? null : v.getClass().getName()), v);
                }
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
        Map binlogData = getBinlogData(row, false);
        if (CollectionUtils.isEmpty(binlogData)) {
            return messageId;
        }
        String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
        String event = (String) row.get(ConfigConstant.DATA_EVENT);
        TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
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
                
                // 执行DDL重做（会自动调用parseDDl方法，更新字段映射）
                bufferActuatorRouter.execute(metaId, ddlChangedEvent);
            } catch (Exception e) {
                logger.error("DDL重做失败：反序列化DDLConfig异常, messageId={}, error={}", messageId, e.getMessage(), e);
                return messageId;
            }
        } else {
            // 数据同步重做：处理ROW事件
            // 有修改同步值
            String retryDataParams = params.get("retryDataParams");
            if (StringUtil.isNotBlank(retryDataParams)) {
                JsonUtil.parseMap(retryDataParams).forEach((k, v) -> binlogData.put(k, convertValue(binlogData.get(k), (String) v)));
            }
            
            // 转换为源字段
            final Picker picker = new Picker(tableGroup);
            List<Object> changedRow = picker.pickSourceData(binlogData);
            RowChangedEvent changedEvent = new RowChangedEvent(sourceTableName, event, changedRow);
            
            // 执行同步是否成功
            bufferActuatorRouter.execute(metaId, changedEvent);
        }
        storageService.remove(StorageEnum.DATA, metaId, messageId);
        // 更新失败数
        Meta meta = profileComponent.getMeta(metaId);
        Assert.notNull(meta, "Meta can not be null.");
        meta.getFail().decrementAndGet();
        meta.setUpdateTime(Instant.now().toEpochMilli());
        profileComponent.editConfigModel(meta);
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

}