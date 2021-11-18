package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.strategy.AbstractFlushStrategy;

import java.util.List;
import java.util.Map;

/**
 * 不记录全量数据, 只记录增量同步数据
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 21:49
 */
public final class DisableFullFlushStrategy extends AbstractFlushStrategy {

    @Override
    public void flushFullData(String metaId, Result writer, String event, List<Map> data) {
        // 不记录全量数据
    }

}