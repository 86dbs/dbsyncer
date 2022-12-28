package org.dbsyncer.biz.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbsyncer.biz.DataSyncService;
import org.dbsyncer.biz.vo.BinlogColumnVo;
import org.dbsyncer.biz.vo.MessageVo;
import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.monitor.Monitor;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.Picker;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.WriterRequest;
import org.dbsyncer.storage.binlog.proto.BinlogMap;
import org.dbsyncer.storage.constant.ConfigConstant;
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
    private Monitor monitor;

    @Resource
    private CacheService cacheService;

    @Resource
    private BufferActuator writerBufferActuator;

    @Override
    public MessageVo getMessageVo(String metaId, String messageId) {
        Assert.hasText(metaId, "The metaId is null.");
        Assert.hasText(messageId, "The messageId is null.");

        MessageVo messageVo = new MessageVo();
        try {
            Map row = monitor.getData(metaId, messageId);
            Map binlogData = getBinlogData(row, true);
            String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
            TableGroup tableGroup = monitor.getTableGroup(tableGroupId);
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
        byte[] bytes = (byte[]) row.get(ConfigConstant.BINLOG_DATA);
        if (null == bytes) {
            if (prettyBytes) {
                String json = (String) row.get(ConfigConstant.CONFIG_MODEL_JSON);
                return JsonUtil.parseObject(json).toJavaObject(Map.class);
            }
            return Collections.EMPTY_MAP;
        }
        BinlogMap message = BinlogMap.parseFrom(bytes);

        // 1、获取配置信息
        final TableGroup tableGroup = cacheService.get(tableGroupId, TableGroup.class);

        // 2、反序列数据
        Map<String, Object> map = new HashMap<>();
        final Picker picker = new Picker(tableGroup.getFieldMapping());
        final Map<String, Field> fieldMap = picker.getSourceFieldMap();
        message.getRowMap().forEach((k, v) -> {
            if (fieldMap.containsKey(k)) {
                Object val = BinlogMessageUtil.deserializeValue(fieldMap.get(k).getType(), v);
                // 处理二进制对象显示
                if (prettyBytes) {
                    if (null != val && val instanceof byte[]) {
                        byte[] b = (byte[]) val;
                        if (b.length > 128) {
                            map.put(k, String.format("byte[%d]", b.length));
                            return;
                        }
                        map.put(k, Arrays.toString(b));
                        return;
                    }
                }
                map.put(k, val);
            }
        });
        return map;
    }

    @Override
    public String sync(Map<String, String> params) {
        String metaId = params.get("metaId");
        String messageId = params.get("messageId");
        Assert.hasText(metaId, "The metaId is null.");
        Assert.hasText(messageId, "The messageId is null.");

        try {
            Map row = monitor.getData(metaId, messageId);
            Map binlogData = getBinlogData(row, false);
            // 历史数据不支持手动同步
            if (CollectionUtils.isEmpty(binlogData)) {
                return messageId;
            }
            String tableGroupId = (String) row.get(ConfigConstant.DATA_TABLE_GROUP_ID);
            String event = (String) row.get(ConfigConstant.DATA_EVENT);
            // 有修改同步值
            String retryDataParams = params.get("retryDataParams");
            if (StringUtil.isNotBlank(retryDataParams)) {
                JsonUtil.parseObject(retryDataParams).getInnerMap().forEach((k, v) -> binlogData.put(k, convertValue(binlogData.get(k), (String) v)));
            }
            writerBufferActuator.offer(new WriterRequest(tableGroupId, event, binlogData));
            monitor.removeData(metaId, messageId);
            // 更新失败数
            Meta meta = cacheService.get(metaId, Meta.class);
            Assert.notNull(meta, "Meta can not be null.");
            meta.getFail().decrementAndGet();
            meta.setUpdateTime(Instant.now().toEpochMilli());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return messageId;
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