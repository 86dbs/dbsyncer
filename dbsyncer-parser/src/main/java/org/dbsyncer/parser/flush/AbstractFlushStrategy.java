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
    public void flushFullData(String metaId, Result result, String event, List<Map> dataList) {
        flush(metaId, result, event, dataList);
    }

    @Override
    public void flushIncrementData(String metaId, Result result, String event, List<Map> dataList) {
        flush(metaId, result, event, dataList);
    }

    protected void flush(String metaId, Result result, String event, List<Map> dataList) {
        refreshTotal(metaId, result, dataList);

        boolean success = 0 == result.getFail().get();
        if (!success) {
            dataList.clear();
            dataList.addAll(result.getFailData());
        }
        flushService.asyncWrite(metaId, event, success, dataList, result.getError().toString());
    }

    protected void refreshTotal(String metaId, Result writer, List<Map> dataList){
        long fail = writer.getFail().get();
        Meta meta = getMeta(metaId);
        meta.getFail().getAndAdd(fail);
        meta.getSuccess().getAndAdd(dataList.size() - fail);
    }

    protected Meta getMeta(String metaId) {
        Assert.hasText(metaId, "Meta id can not be empty.");
        Meta meta = cacheService.get(metaId, Meta.class);
        Assert.notNull(meta, "Meta can not be null.");
        return meta;
    }

}
