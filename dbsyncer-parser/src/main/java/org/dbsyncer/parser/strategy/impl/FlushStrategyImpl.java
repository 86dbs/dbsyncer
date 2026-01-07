/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.CacheService;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2021-11-18 22:22
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
    public void flushFullData(String metaId, Result result, String event) {
        // 不记录全量数据, 只记录增量同步数据, 将异常记录到系统日志中
        if (!profileComponent.getSystemConfig().isEnableStorageWriteFull()) {
            // 不记录全量数据，只统计成功失败总数
            refreshTotal(metaId, result);

            if (!CollectionUtils.isEmpty(result.getFailData())) {
                logger.error(result.error);
                LogType logType = LogType.TableGroupLog.FULL_FAILED;
                logService.log(logType, "%s:%s:failed num:%d:last error:%s", result.getTargetTableGroupName(), logType.getMessage(), result.getFailData().size(), result.error);
            }
            return;
        }

        flush(metaId, result, event);
    }

    @Override
    public void flushIncrementData(String metaId, Result result, String event) {
        // 为增量设置总数
        Meta meta = cacheService.get(metaId, Meta.class);
        AtomicLong total = meta.getTotal();
        total.getAndAdd(result.getFailData().size());
        total.getAndAdd(result.getSuccessData().size());

        // 更新TableGroup的增量同步数量
        if (result.getTableGroupId() != null) {
            try {
                TableGroup tableGroup = profileComponent.getTableGroup(result.getTableGroupId());
                if (tableGroup != null) {
                    tableGroup.setIncrementSuccess(tableGroup.getIncrementSuccess() + result.getSuccessData().size());
                    profileComponent.editConfigModel(tableGroup);
                }
            } catch (Exception e) {
                logger.error("更新TableGroup增量同步数量失败", e);
            }
        }

        flush(metaId, result, event);
    }

    private void asyncWrite(String metaId, String tableGroupId, String targetTableGroupName, String event, boolean success, List<Map> data, String error) {
        long now = Instant.now().toEpochMilli();
        data.forEach(r -> {
            Map<String, Object> row = new HashMap();
            row.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
            row.put(ConfigConstant.DATA_SUCCESS, success ? StorageDataStatusEnum.SUCCESS.getValue() : StorageDataStatusEnum.FAIL.getValue());
            row.put(ConfigConstant.DATA_TABLE_GROUP_ID, tableGroupId);
            row.put(ConfigConstant.DATA_TARGET_TABLE_NAME, targetTableGroupName);
            row.put(ConfigConstant.DATA_EVENT, event);
            row.put(ConfigConstant.DATA_ERROR, error);
            row.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            try {
                byte[] bytes = BinlogMessageUtil.toBinlogMap(r).toByteArray();
                row.put(ConfigConstant.BINLOG_DATA, bytes);
            } catch (Exception e) {
                logger.warn("可能存在Blob或inputStream大文件类型, 无法序列化:{}", r);
            }
            storageBufferActuator.offer(new StorageRequest(metaId, row));
        });
    }

    private void refreshTotal(String metaId, Result writer) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        if (meta != null) {
            // 更新Meta的总数
            meta.getFail().getAndAdd(writer.getFailData().size());
            meta.getSuccess().getAndAdd(writer.getSuccessData().size());
            meta.setUpdateTime(Instant.now().toEpochMilli());

            // 更新TableGroup的进度
            if (writer.getTableGroupId() != null) {
                try {
                    TableGroup tableGroup = profileComponent.getTableGroup(writer.getTableGroupId());
                    if (tableGroup != null) {
                        // 更新全量和增量的成功/失败计数
                        tableGroup.setSuccess(tableGroup.getSuccess() + writer.getSuccessData().size());
                        tableGroup.setFail(tableGroup.getFail() + writer.getFailData().size());
                        tableGroup.setFullSuccess(tableGroup.getFullSuccess() + writer.getSuccessData().size());
                        tableGroup.setFullFail(tableGroup.getFullFail() + writer.getFailData().size());

                        // 动态调整总数（只调整全量总数）
                        long completed = tableGroup.getFullSuccess() + tableGroup.getFullFail();
                        if (tableGroup.getTotal() < completed) {
                            tableGroup.setTotal(completed);
                        }

                        // 如果estimatedTotal为0，则设置为sourceTable.getCount()（用于首次计算进度）
                        if (tableGroup.getEstimatedTotal() == 0 && tableGroup.getSourceTable() != null && tableGroup.getSourceTable().getCount() > 0) {
                            tableGroup.setEstimatedTotal(tableGroup.getSourceTable().getCount());
                        } else if (tableGroup.getEstimatedTotal() == 0) {
                            // 如果sourceTable.getCount()为0，则使用total作为estimatedTotal
                            tableGroup.setEstimatedTotal(tableGroup.getTotal());
                        }

                        // 更新状态：如果fail > 0，状态为"异常"，否则为"正常"
                        tableGroup.setStatus(tableGroup.getFail() > 0 ? "异常" : "正常");
                        profileComponent.editConfigModel(tableGroup);
                    }
                } catch (Exception e) {
                    logger.error("更新TableGroup进度失败", e);
                }
            }
        }
    }

    private void flush(String metaId, Result result, String event) {
        refreshTotal(metaId, result);

        SystemConfig systemConfig = profileComponent.getSystemConfig();
        // 是否写失败数据
        if (systemConfig.isEnableStorageWriteFail() && !CollectionUtils.isEmpty(result.getFailData())) {
            asyncWrite(metaId, result.getTableGroupId(), result.getTargetTableGroupName(), event, false, result.getFailData(), result.error);
        }

        // 是否写成功数据
        if (systemConfig.isEnableStorageWriteSuccess() && !CollectionUtils.isEmpty(result.getSuccessData())) {
            asyncWrite(metaId, result.getTableGroupId(), result.getTargetTableGroupName(), event, true, result.getSuccessData(), "");
        }
    }

}