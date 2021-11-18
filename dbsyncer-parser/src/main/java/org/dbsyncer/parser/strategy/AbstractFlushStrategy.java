package org.dbsyncer.parser.strategy;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.flush.FlushService;
import org.dbsyncer.parser.model.Meta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Queue;

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

    @Override
    public void flushFullData(String metaId, Result writer, String event, List<Map> data) {
        flush(metaId, writer, event, data);
    }

    @Override
    public void flushIncrementData(String metaId, Result writer, String event, List<Map> data) {
        flush(metaId, writer, event, data);
    }

    protected void flush(String metaId, Result writer, String event, List<Map> data) {
        // 引用传递
        long total = data.size();
        long fail = writer.getFail().get();
        Meta meta = getMeta(metaId);
        meta.getFail().getAndAdd(fail);
        meta.getSuccess().getAndAdd(total - fail);

        // 记录错误数据
        Queue<Map> failData = writer.getFailData();
        boolean success = CollectionUtils.isEmpty(failData);
        if (!success) {
            data.clear();
            data.addAll(failData);
        }
        String error = writer.getError().toString();
        flushService.asyncWrite(metaId, event, success, data, error);
    }

    protected Meta getMeta(String metaId) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        Assert.notNull(meta, "Meta can not be null.");
        return meta;
    }

}
