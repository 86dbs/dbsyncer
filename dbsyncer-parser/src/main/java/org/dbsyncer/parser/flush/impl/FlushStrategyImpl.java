/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.CacheService;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.FlushService;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 22:22
 */
@Component
public final class FlushStrategyImpl implements FlushStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private FlushService flushService;

    @Resource
    private CacheService cacheService;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Override
    public void flushFullData(String metaId, Result result, String event) {
        // 不记录全量数据, 只记录增量同步数据, 将异常记录到系统日志中
        if (!getSystemConfig().isEnableStorageWriteFull()) {
            // 不记录全量数据，只统计成功失败总数
            refreshTotal(metaId, result);

            if (!CollectionUtils.isEmpty(result.getFailData())) {
                logger.error(result.getError().toString());
                LogType logType = LogType.TableGroupLog.FULL_FAILED;
                logService.log(logType, "%s:%s:%s", result.getTargetTableGroupName(), logType.getMessage(), result.getError().toString());
            }
            return;
        }

        flush(metaId, result, event);
    }

    @Override
    public void flushIncrementData(String metaId, Result result, String event) {
        flush(metaId, result, event);
    }

    protected void refreshTotal(String metaId, Result writer) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        if (meta != null) {
            meta.getFail().getAndAdd(writer.getFailData().size());
            meta.getSuccess().getAndAdd(writer.getSuccessData().size());
            meta.setUpdateTime(Instant.now().toEpochMilli());
        }
    }

    private void flush(String metaId, Result result, String event) {
        refreshTotal(metaId, result);

        // 是否写失败数据
        if (getSystemConfig().isEnableStorageWriteFail() && !CollectionUtils.isEmpty(result.getFailData())) {
            final String error = StringUtil.substring(result.getError().toString(), 0, getSystemConfig().getMaxStorageErrorLength());
            flushService.asyncWrite(metaId, result.getTableGroupId(), result.getTargetTableGroupName(), event, false, result.getFailData(), error);
        }

        // 是否写成功数据
        if (getSystemConfig().isEnableStorageWriteSuccess() && !CollectionUtils.isEmpty(result.getSuccessData())) {
            flushService.asyncWrite(metaId, result.getTableGroupId(), result.getTargetTableGroupName(), event, true, result.getSuccessData(), "");
        }
    }

    private SystemConfig getSystemConfig() {
        return profileComponent.getSystemConfig();
    }

}