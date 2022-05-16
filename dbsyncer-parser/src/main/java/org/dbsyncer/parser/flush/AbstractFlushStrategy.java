package org.dbsyncer.parser.flush;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Meta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 22:22
 */
public abstract class AbstractFlushStrategy implements FlushStrategy {

    private static final int MAX_ERROR_LENGTH = 1000;

    @Autowired
    private FlushService flushService;

    @Autowired
    private CacheService cacheService;

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

        if (!CollectionUtils.isEmpty(result.getFailData())) {
            final String error = StringUtil.substring(result.getError().toString(), 0, MAX_ERROR_LENGTH);
            flushService.write(metaId, event, false, result.getFailData(), error);
        }
        if (!CollectionUtils.isEmpty(result.getSuccessData())) {
            flushService.write(metaId, event, true, result.getSuccessData(), "");
        }
    }

    protected void refreshTotal(String metaId, Result writer) {
        Meta meta = getMeta(metaId);
        meta.getFail().getAndAdd(writer.getFailData().size());
        meta.getSuccess().getAndAdd(writer.getSuccessData().size());
    }

    protected Meta getMeta(String metaId) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        Assert.notNull(meta, "Meta can not be null.");
        return meta;
    }

}