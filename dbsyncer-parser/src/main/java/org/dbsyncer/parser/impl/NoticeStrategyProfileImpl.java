/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.event.RemoveNoticeStrategyCacheEvent;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.AbstractConfigModelProfile;
import org.dbsyncer.parser.NoticeStrategyProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.NoticeStrategyConfig;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * {@link NoticeStrategyProfile} 实现：单条读走 {@link AbstractConfigModelProfile} 缓存，
 * 写后本地失效并通知集群其它节点。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class NoticeStrategyProfileImpl extends AbstractConfigModelProfile<NoticeStrategyConfig>
        implements NoticeStrategyProfile, ApplicationListener<RemoveNoticeStrategyCacheEvent> {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Override
    public List<NoticeStrategyConfig> listAll() {
        return operationTemplate.queryList(StorageEnum.NOTICE_STRATEGY, new Query(), NoticeStrategyConfig.class);
    }

    @Override
    public NoticeStrategyConfig get(String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        return getCache(id);
    }

    @Override
    public String add(NoticeStrategyConfig config) {
        String id = operationTemplate.execute(config, CommandEnum.OPR_ADD);
        // 新增也需通知集群，其它节点重建策略运行态
        removeCacheAndNotice(id);
        return id;
    }

    @Override
    public String update(NoticeStrategyConfig config) {
        String id = operationTemplate.execute(config, CommandEnum.OPR_EDIT);
        removeCacheAndNotice(id);
        return id;
    }

    @Override
    public void remove(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.NOTICE_STRATEGY, id);
        removeCacheAndNotice(id);
    }

    @Override
    public void onApplicationEvent(RemoveNoticeStrategyCacheEvent event) {
        removeCache(event.getCommonMessage().getId());
    }
}
