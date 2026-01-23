/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.consumer;

import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.QuartzListenerContext;
import org.dbsyncer.sdk.listener.Watcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public final class ParserConsumer implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(ParserConsumer.class);
    
    private final BufferActuatorRouter bufferActuatorRouter;
    private final ProfileComponent profileComponent;
    private final PluginFactory pluginFactory;
    private final LogService logService;
    private final String metaId;

    public ParserConsumer(BufferActuatorRouter bufferActuatorRouter, ProfileComponent profileComponent, PluginFactory pluginFactory, LogService logService, String metaId, List<TableGroup> tableGroups) {
        this.bufferActuatorRouter = bufferActuatorRouter;
        this.profileComponent = profileComponent;
        this.pluginFactory = pluginFactory;
        this.logService = logService;
        this.metaId = metaId;
        // 注册到路由服务中
        bufferActuatorRouter.getOrCreateActuator(metaId);
    }

    @Override
    public void changeEventBefore(QuartzListenerContext context) {
        pluginFactory.process(context, ProcessEnum.BEFORE);
    }

    @Override
    public void changeEvent(ChangedEvent event) {
        // 所有到达 changeEvent 的事件都是任务事件（已通过过滤和配置检查）
        // execute() 方法会自动设置 pending 状态
        bufferActuatorRouter.execute(metaId, event);
    }

    @Override
    public void flushEvent(Map<String, String> snapshot) throws Exception {
        Meta meta = profileComponent.getMeta(metaId);
        if (meta != null) {
            meta.setSnapshot(snapshot);
            // 更新 updateTime 以记录持久化时间
            meta.setUpdateTime(Instant.now().toEpochMilli());
            profileComponent.editConfigModel(meta);
            
            // 持久化发生变化的 TableGroup（增量场景）
            // 由于在 Meta 中记录变化的表已经非常准确，无需额外的计数比较
            Set<String> changedTableGroupIds = meta.getChangedTableGroupIds();
            if (!changedTableGroupIds.isEmpty()) {
                for (String tableGroupId : changedTableGroupIds) {
                    try {
                        TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
                        if (tableGroup != null) {
                            profileComponent.editConfigModel(tableGroup);
                        }
                    } catch (Exception e) {
                        logger.error("持久化 TableGroup 失败: tableGroupId={}", tableGroupId, e);
                    }
                }
                // 持久化完成后清空标记
                meta.clearChangedTableGroupIds();
            }
        }
    }

    @Override
    public void errorEvent(Exception e) {
        logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
    }

    @Override
    public long getMetaUpdateTime() {
        Meta meta = profileComponent.getMeta(metaId);
        return meta != null ? meta.getUpdateTime() : 0L;
    }

    @Override
    public boolean hasPendingTask() {
        // 查询该 meta 的 pending 状态
        return bufferActuatorRouter.hasPendingTask(metaId);
    }
}