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
        if (meta != null) {
            AtomicLong total = meta.getTotal();
            total.getAndAdd(result.getFailData().size());
            total.getAndAdd(result.getSuccessData().size());

            // 更新TableGroup的增量同步数量
            updateTableGroupIncrementProgress(result);
            
            // 更新Meta的同步计数
            updateMetaCounts(meta, result);
        }

        // 直接调用asyncWrite方法，不调用flush方法，避免更新fullSuccess和fullFail字段
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

    /**
     * 更新TableGroup的增量同步进度
     *
     * @param result 同步结果，包含成功和失败的数据
     */
    private void updateTableGroupIncrementProgress(Result result) {
        if (result.getTableGroupId() == null) {
            return;
        }

        try {
            TableGroup tableGroup = profileComponent.getTableGroup(result.getTableGroupId());
            if (tableGroup != null) {
                tableGroup.setIncrementSuccess(tableGroup.getIncrementSuccess() + result.getSuccessData().size());
                tableGroup.setIncrementFail(tableGroup.getIncrementFail() + result.getFailData().size());
                profileComponent.editConfigModel(tableGroup);
            }
        } catch (Exception e) {
            logger.error("更新TableGroup增量同步数量失败", e);
        }
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
        if (meta == null) {
            return;
        }

        // 更新Meta的总数
        updateMetaCounts(meta, writer);

        // 更新TableGroup的进度
        if (writer.getTableGroupId() != null) {
            updateTableGroupProgress(writer.getTableGroupId(), writer);
        }
    }

    /**
     * 更新Meta的同步计数
     *
     * @param meta   元数据对象，包含全局同步计数
     * @param writer 同步结果，包含成功和失败的数据
     */
    private void updateMetaCounts(Meta meta, Result writer) {
        meta.getFail().getAndAdd(writer.getFailData().size());
        meta.getSuccess().getAndAdd(writer.getSuccessData().size());
        meta.setUpdateTime(Instant.now().toEpochMilli());
    }

    /**
     * 更新TableGroup的同步进度
     *
     * @param tableGroupId 表组ID
     * @param writer       同步结果，包含成功和失败的数据
     */
    private void updateTableGroupProgress(String tableGroupId, Result writer) {
        TableGroup tableGroup = getTableGroup(tableGroupId);
        if (tableGroup == null) {
            return;
        }

        try {
            // 更新计数
            updateTableGroupCounts(tableGroup, writer);
            
            // 动态调整总数
            adjustTotalCount(tableGroup);
            
            // 设置预计总数
            setEstimatedTotal(tableGroup);
            
            // 更新状态
            updateTableGroupStatus(tableGroup);
            
            profileComponent.editConfigModel(tableGroup);
        } catch (Exception e) {
            logger.error("更新TableGroup进度失败", e);
        }
    }

    /**
     * 获取TableGroup实例
     *
     * @param tableGroupId 表组ID
     * @return TableGroup实例，如果获取失败返回null
     */
    private TableGroup getTableGroup(String tableGroupId) {
        try {
            return profileComponent.getTableGroup(tableGroupId);
        } catch (Exception e) {
            logger.error("获取TableGroup失败", e);
            return null;
        }
    }

    /**
     * 更新TableGroup的同步计数
     *
     * @param tableGroup 表组实例
     * @param writer     同步结果，包含成功和失败的数据
     */
    private void updateTableGroupCounts(TableGroup tableGroup, Result writer) {
        int successSize = writer.getSuccessData().size();
        int failSize = writer.getFailData().size();
        
        tableGroup.setSuccess(tableGroup.getSuccess() + successSize);
        tableGroup.setFail(tableGroup.getFail() + failSize);
        tableGroup.setFullSuccess(tableGroup.getFullSuccess() + successSize);
        tableGroup.setFullFail(tableGroup.getFullFail() + failSize);
    }

    /**
     * 动态调整TableGroup的总数
     * 当已完成的同步数量超过当前总数时，更新总数为已完成数量
     *
     * @param tableGroup 表组实例
     */
    private void adjustTotalCount(TableGroup tableGroup) {
        long completed = tableGroup.getFullSuccess() + tableGroup.getFullFail();
        if (tableGroup.getTotal() < completed) {
            tableGroup.setTotal(completed);
        }
    }

    /**
     * 设置TableGroup的预计总数
     * 如果预计总数未设置，则根据源表计数或当前总数来设置
     *
     * @param tableGroup 表组实例
     */
    private void setEstimatedTotal(TableGroup tableGroup) {
        if (tableGroup.getEstimatedTotal() != 0) {
            return;
        }

        if (tableGroup.getSourceTable() != null && tableGroup.getSourceTable().getCount() > 0) {
            tableGroup.setEstimatedTotal(tableGroup.getSourceTable().getCount());
        } else {
            tableGroup.setEstimatedTotal(tableGroup.getTotal());
        }
    }

    /**
     * 更新TableGroup的同步状态
     * 根据失败计数判断同步状态：有失败记录则为"异常"，否则为"正常"
     *
     * @param tableGroup 表组实例
     */
    private void updateTableGroupStatus(TableGroup tableGroup) {
        tableGroup.setStatus(tableGroup.getFail() > 0 ? "异常" : "正常");
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