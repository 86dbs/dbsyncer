package org.dbsyncer.parser.flush;

import org.dbsyncer.common.model.Result;

/**
 * 记录同步数据策略
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/29 22:38
 */
public interface FlushStrategy {

    /**
     * 记录全量同步数据
     *
     * @param metaId
     * @param result
     * @param event
     */
    void flushFullData(String metaId, Result result, String event);

    /**
     * 记录增量同步数据
     *
     * @param metaId
     * @param result
     * @param event
     */
    void flushIncrementData(String metaId, Result result, String event);

}