package org.dbsyncer.parser.strategy;

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
     * @param writer
     * @param event
     * @param data
     */
    void flushFullData(String metaId, Result writer, String event, List<Map> data);

    /**
     * 记录增量同步数据
     *
     * @param metaId
     * @param writer
     * @param event
     * @param data
     */
    void flushIncrementData(String metaId, Result writer, String event, List<Map> data);

}