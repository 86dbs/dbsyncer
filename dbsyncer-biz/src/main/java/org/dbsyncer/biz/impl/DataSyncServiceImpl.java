package org.dbsyncer.biz.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.vo.BinlogColumnVo;
import org.dbsyncer.biz.vo.MessageVo;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.IndexFieldResolverEnum;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.query.Query;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    public Map getBinlogData(Map row, boolean prettyBytes) throws InvalidProtocolBufferException {
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

        // 3、反序列
        Map<String, Object> target = new HashMap<>();
        final Picker picker = new Picker(tableGroup.getFieldMapping());
        final Map<String, Field> fieldMap = picker.getTargetFieldMap();
        BinlogMap message = BinlogMap.parseFrom(bytes);
        message.getRowMap().forEach((k, v) -> {
            if (fieldMap.containsKey(k)) {
                try {
                    Object val = BinlogMessageUtil.deserializeValue(fieldMap.get(k).getType(), v);
                    // 处理二进制对象显示
                    if (prettyBytes) {
                        if (null != val && val instanceof byte[]) {
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
    public String sync(Map<String, String> params) {
        String metaId = params.get("metaId");
        String messageId = params.get("messageId");
        Assert.hasText(metaId, "The metaId is null.");
        Assert.hasText(messageId, "The messageId is null.");

        try {
            Map row = getData(metaId, messageId);
            Map binlogData = getBinlogData(row, false);
            if (CollectionUtils.isEmpty(binlogData)) {
                return messageId;
            }
            String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
            String event = (String) row.get(ConfigConstant.DATA_EVENT);
            // 有修改同步值
            String retryDataParams = params.get("retryDataParams");
            if (StringUtil.isNotBlank(retryDataParams)) {
                JsonUtil.parseMap(retryDataParams).forEach((k, v) -> binlogData.put(k, convertValue(binlogData.get(k), (String) v)));
            }
            TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
            String sourceTableName = tableGroup.getSourceTable().getName();
            RowChangedEvent changedEvent = new RowChangedEvent(sourceTableName, event, Collections.EMPTY_LIST);
            // 转换为源字段
            final Picker picker = new Picker(tableGroup.getFieldMapping());
            changedEvent.setChangedRow(picker.pickSourceData(binlogData));
            bufferActuatorRouter.execute(metaId, tableGroupId, changedEvent);
            storageService.remove(StorageEnum.DATA, metaId, messageId);
            // 更新失败数
            Meta meta = profileComponent.getMeta(metaId);
            Assert.notNull(meta, "Meta can not be null.");
            meta.getFail().decrementAndGet();
            meta.setUpdateTime(Instant.now().toEpochMilli());
            profileComponent.editConfigModel(meta);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return messageId;
    }

    private Map getData(String metaId, String messageId) {
        Query query = new Query(1, 1);
        Map<String, IndexFieldResolverEnum> fieldResolvers = new LinkedHashMap<>();
        fieldResolvers.put(ConfigConstant.BINLOG_DATA, IndexFieldResolverEnum.BINARY);
        query.setIndexFieldResolverMap(fieldResolvers);
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
            String[] split = StringUtil.split(s, ",");
            int length = split.length;
            b = new byte[length];
            for (int i = 0; i < length; i++) {
                b[i] = Byte.valueOf(split[i].trim());
            }
        }
        return b;
    }

}