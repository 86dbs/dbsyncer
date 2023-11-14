package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.flush.AbstractFlushStrategy;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

/**
 * 不记录全量数据, 只记录增量同步数据, 将异常记录到系统日志中
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 21:49
 */
public final class DisableFullFlushStrategy extends AbstractFlushStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private LogService logService;

    @Override
    public void flushFullData(String metaId, Result result, String event) {
        // 不记录全量数据，只统计成功失败总数
        refreshTotal(metaId, result);

        if (!CollectionUtils.isEmpty(result.getFailData())) {
            logger.error(result.getError().toString());
            LogType logType = LogType.TableGroupLog.FULL_FAILED;
            logService.log(logType, "%s:%s:%s", result.getTargetTableGroupName(), logType.getMessage(), result.getError().toString());
        }
    }

}