/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.event.FullRefreshEvent;
import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Task;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.FullFlushService;
import org.springframework.context.ApplicationContext;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 单机全量页级落库：刷 Meta/明细并可选发布进度事件。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14
 */
public class FullFlushServiceImpl implements FullFlushService {

    @Resource
    private FlushStrategy flushStrategy;

    @Resource
    private ApplicationContext applicationContext;

    @Override
    public void flush(Task task, Result result, SchemaResolver targetSchemaResolver, Map<String, Field> targetFieldMap) {
        result.setTaskId(task.getId());
        result.setEvent(ConnectorConstant.OPERTION_INSERT);
        flushStrategy.flushFullData(result, targetSchemaResolver, targetFieldMap);
        if (!task.isSkipTableProgressEvent()) {
            applicationContext.publishEvent(new FullRefreshEvent(applicationContext, task));
        }
    }
}
