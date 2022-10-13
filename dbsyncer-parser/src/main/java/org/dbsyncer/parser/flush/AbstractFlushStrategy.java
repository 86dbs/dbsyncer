package org.dbsyncer.parser.flush;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.config.IncrementDataConfig;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 22:22
 */
public abstract class AbstractFlushStrategy implements FlushStrategy {

    @Autowired
    private FlushService flushService;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private IncrementDataConfig flushDataConfig;

    @Override
    public void flushFullData(String metaId, Result result, String event) {
        flush(metaId, result, event);
    }

    @Override
    public void flushIncrementData(String metaId, Result result, String event) {
        flush(metaId, result, event);
    }

    protected void flush(String metaId, Result result, String event) {
        refreshTotal(metaId, result);

        if (flushDataConfig.isWriterFail() && !CollectionUtils.isEmpty(result.getFailData())) {
            final String error = StringUtil.substring(result.getError().toString(), 0, flushDataConfig.getMaxErrorLength());
            flushService.write(metaId, event, false, result.getFailData(), error);
        }

        // 是否写增量数据
        if (flushDataConfig.isWriterSuccess() && !CollectionUtils.isEmpty(result.getSuccessData())) {
            flushService.write(metaId, event, true, result.getSuccessData(), "");
        }
    }

    protected void refreshTotal(String metaId, Result writer) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        Assert.notNull(meta, "Meta can not be null.");
        meta.getFail().getAndAdd(writer.getFailData().size());
        meta.getSuccess().getAndAdd(writer.getSuccessData().size());
    }

}