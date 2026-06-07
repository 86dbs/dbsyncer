/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.strategy.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.common.binlog.proto.BinlogMap;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.CacheService;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0.0
 * @author AE86
 * @date 2021-11-18 22:22
 */
@Component
public final class FlushStrategyImpl implements FlushStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private CacheService cacheService;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Resource
    private BufferActuator storageBufferActuator;

    @Override
    public void flushFullData(Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap) {
        // 不记录全量数据, 只记录增量同步数据, 将异常记录到系统日志中
        if (!profileComponent.getSystemConfig().isEnableStorageWriteFull()) {
            // 不记录全量数据，只统计成功失败总数
            refreshTotal(result);

            if (!CollectionUtils.isEmpty(result.getFailData())) {
                logger.error(result.getError().toString());
                LogType logType = LogType.TableGroupLog.FULL_FAILED;
                logService.log(logType, "%s:%s:%s", result.getTargetTableGroupName(), logType.getMessage(), result.getError().toString());
            }
            return;
        }

        flush(result, targetSchemaResolver, targetFieldMap);
    }

    @Override
    public void flushIncrementData(Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap) {
        flush(result, targetSchemaResolver, targetFieldMap);
    }

    private void asyncWrite(Result result, SchemaResolver schemaResolver, Map<String, Field> targetFieldMap, boolean success, List<Map> data, String error) {
        String metaId = result.getMetaId();
        String event = result.getEvent();
        String tableGroupId = result.getTableGroupId();
        String targetTableGroupName = result.getTargetTableGroupName();

        long now = Instant.now().toEpochMilli();
        data.forEach(r-> {
            Map<String, Object> row = new HashMap<>();
            row.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
            row.put(ConfigConstant.DATA_SUCCESS, success ? StorageDataStatusEnum.SUCCESS.getValue() : StorageDataStatusEnum.FAIL.getValue());
            row.put(ConfigConstant.DATA_TABLE_GROUP_ID, tableGroupId);
            row.put(ConfigConstant.DATA_TARGET_TABLE_NAME, targetTableGroupName);
            row.put(ConfigConstant.DATA_EVENT, event);
            row.put(ConfigConstant.DATA_ERROR, error);
            row.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            try {
                row.put(ConfigConstant.BINLOG_DATA, toBinlogBytes(schemaResolver, r, targetFieldMap));
            } catch (Exception e) {
                // 构建详细类型信息
                StringBuilder typeInfo = new StringBuilder();
                r.forEach((k, val) -> {
                    typeInfo.append(k).append("=").append(val == null ? "null" : val.getClass().getName()).append("; ");
                });
                logger.warn("可能存在Blob或inputStream大文件类型, 无法序列化。字段类型详情: {}", typeInfo, e);
            }
            storageBufferActuator.offer(new StorageRequest(metaId, row));
        });
    }

    private byte[] toBinlogBytes(SchemaResolver schemaResolver, Map<String, Object> data, Map<String, Field> fieldMap) {
        BinlogMap.Builder dataBuilder = BinlogMap.newBuilder();
        data.forEach((k, v)-> {
            if (null != v) {
                // DDL
                if (fieldMap == null) {
                    ByteString bytes = schemaResolver.serialize(v, null);
                    if (null != bytes) {
                        dataBuilder.putRow(k, bytes);
                    }
                    return;
                }

                // DML
                Field field = fieldMap.get(k);
                if (field != null) {
                    ByteString bytes = schemaResolver.serialize(v, field);
                    if (null != bytes) {
                        dataBuilder.putRow(k, bytes);
                    }
                }
            }
        });
        return dataBuilder.build().toByteArray();
    }

    private void refreshTotal(Result writer) {
        Meta meta = cacheService.get(writer.getMetaId(), Meta.class);
        if (meta != null) {
            meta.getFail().getAndAdd(writer.getFailData().size());
            meta.getSuccess().getAndAdd(writer.getSuccessData().size());
            meta.setUpdateTime(Instant.now().toEpochMilli());
        }
    }

    private void flush(Result result, SchemaResolver schemaResolver, Map<String, Field> targetFieldMap) {
        refreshTotal(result);

        SystemConfig systemConfig = profileComponent.getSystemConfig();
        // 是否写失败数据
        if (systemConfig.isEnableStorageWriteFail() && !CollectionUtils.isEmpty(result.getFailData())) {
            final String error = StringUtil.substring(result.getError().toString(), 0, systemConfig.getMaxStorageErrorLength());
            asyncWrite(result, schemaResolver, targetFieldMap, false, result.getFailData(), error);
        }

        // 是否写成功数据
        if (systemConfig.isEnableStorageWriteSuccess() && !CollectionUtils.isEmpty(result.getSuccessData())) {
            asyncWrite(result, schemaResolver, targetFieldMap, true, result.getSuccessData(), "");
        }
    }

}