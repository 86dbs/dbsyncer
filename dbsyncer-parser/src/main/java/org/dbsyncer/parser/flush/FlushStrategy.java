package org.dbsyncer.parser.flush;

import org.dbsyncer.common.model.Result;

import java.util.List;
import java.util.Map;

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
     * @param dataList
     */
    void flushFullData(String metaId, Result result, String event, List<Map> dataList);

    /**
     * 记录增量同步数据
     *
     * @param metaId
     * @param result
     * @param event
     * @param dataList
     */
    void flushIncrementData(String metaId, Result result, String event, List<Map> dataList);

}