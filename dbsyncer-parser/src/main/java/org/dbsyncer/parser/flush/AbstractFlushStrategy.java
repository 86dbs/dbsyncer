package org.dbsyncer.parser.flush;

import org.dbsyncer.cache.CacheService;
import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.model.Meta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

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
        refreshTotal(metaId, writer, data);

        boolean success = 0 == writer.getFail().get();
        if (!success) {
            data.clear();
            data.addAll(writer.getFailData());
        }
        flushService.asyncWrite(metaId, event, success, data, writer.getError().toString());
    }

    protected void refreshTotal(String metaId, Result writer, List<Map> data){
        long fail = writer.getFail().get();
        Meta meta = getMeta(metaId);
        meta.getFail().getAndAdd(fail);
        meta.getSuccess().getAndAdd(data.size() - fail);
    }

    protected Meta getMeta(String metaId) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        Assert.notNull(meta, "Meta can not be null.");
        return meta;
    }

}
