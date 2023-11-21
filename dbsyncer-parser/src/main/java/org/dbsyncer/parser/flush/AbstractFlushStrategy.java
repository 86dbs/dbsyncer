package org.dbsyncer.parser.flush;

import org.dbsyncer.common.config.StorageConfig;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.CacheService;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 22:22
 */
public abstract class AbstractFlushStrategy implements FlushStrategy {

    @Resource
    private FlushService flushService;

    @Resource
    private CacheService cacheService;

    @Resource
    private StorageConfig storageConfig;

    @Override
    public void flushFullData(String metaId, Result result, String event) {
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
        if (storageConfig.isWriteFail() && !CollectionUtils.isEmpty(result.getFailData())) {
            final String error = StringUtil.substring(result.getError().toString(), 0, storageConfig.getMaxErrorLength());
            flushService.asyncWrite(metaId, result.getTableGroupId(), result.getTargetTableGroupName(), event, false, result.getFailData(), error);
        }

        // 是否写成功数据
        if (storageConfig.isWriteSuccess() && !CollectionUtils.isEmpty(result.getSuccessData())) {
            flushService.asyncWrite(metaId, result.getTableGroupId(), result.getTargetTableGroupName(), event, true, result.getSuccessData(), "");
        }
    }

}