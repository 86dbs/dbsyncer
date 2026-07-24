/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.parser.model.Meta;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * {@link MetaProfile} 实现（dbsyncer_meta）。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class MetaProfileImpl implements MetaProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Override
    public Meta getMeta(String metaId) {
        return operationTemplate.queryObject(Meta.class, metaId);
    }

    @Override
    public List<Meta> getMetaAll() {
        return operationTemplate.queryAll(Meta.class);
    }

    @Override
    public List<Meta> getTaskMetaAll() {
        Query condition = new Query();
        condition.addFilter(ConfigConstant.META_IS_TASK_DETAIL, 0);
        return operationTemplate.queryList(StorageEnum.META, condition, Meta.class);
    }

    @Override
    public Meta getMetaByTaskId(String refId, TaskLevelEnum taskLevelEnum) {
        return operationTemplate.getMetaByTaskId(refId, taskLevelEnum);
    }

    @Override
    public Map<String, Meta> getDetailMetaMap(List<String> refIds) {
        return operationTemplate.queryDetailMetaMap(refIds);
    }

    @Override
    public void incrementMeta(MetaIncrement increment) {
        operationTemplate.incrementMeta(increment);
    }
}
