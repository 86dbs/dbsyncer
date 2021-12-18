package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.strategy.AbstractFlushStrategy;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * 不记录全量数据, 只记录增量同步数据, 将异常记录到系统日志中
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 21:49
 */
public final class DisableFullFlushStrategy extends AbstractFlushStrategy {

    @Autowired
    private LogService logService;

    @Override
    public void flushFullData(String metaId, Result writer, String event, List<Map> data) {
        // 不记录全量数据，只统计成功失败总数
        refreshTotal(metaId, writer, data);

        if (0 < writer.getFail().get()) {
            LogType logType = LogType.TableGroupLog.FULL_FAILED;
            logService.log(logType, "%s:%s", logType.getMessage(), writer.getError().toString());
        }
    }

}