/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.consumer;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.TaskMetaProfile;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.QuartzListenerContext;
import org.dbsyncer.sdk.listener.Watcher;
import org.dbsyncer.sdk.spi.BufferActuatorRouterService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023-11-12 01:32
 */
@Component
public final class ParserConsumer implements Watcher, Cloneable {

    @Resource
    private BufferActuatorRouterService bufferActuatorRouter;

    @Resource
    private TaskMetaProfile taskMetaProfile;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private LogService logService;

    private String taskId;

    public void build(String taskId, List<TableGroup> tableGroups, int channelSize) {
        this.taskId = taskId;
        bufferActuatorRouter.bind(taskId, extractSourceTableNames(tableGroups), channelSize);
    }

    @Override
    public void changeEventBefore(QuartzListenerContext context) {
        pluginFactory.process(context, ProcessEnum.BEFORE);
    }

    @Override
    public void changeEvent(ChangedEvent event) {
        bufferActuatorRouter.execute(taskId, event);
    }

    @Override
    public void flushEvent(Map<String, String> snapshot) {
        Meta meta = taskMetaProfile.getMeta(taskId);
        if (meta != null) {
            meta.setSnapshot(snapshot);
            taskMetaProfile.updateMeta(meta);
        }
    }

    @Override
    public void errorEvent(Exception e) {
        logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
    }

    @Override
    public long getMetaUpdateTime() {
        Meta meta = taskMetaProfile.getMeta(taskId);
        return meta != null ? meta.getUpdateTime() : 0L;
    }

    @Override
    public ParserConsumer clone() {
        try {
            return (ParserConsumer) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    private List<String> extractSourceTableNames(List<TableGroup> tableGroups) {
        List<String> tableNames = new ArrayList<>();
        if (tableGroups == null) {
            return tableNames;
        }
        for (TableGroup tableGroup : tableGroups) {
            if (tableGroup == null || tableGroup.getSourceTable() == null
                    || StringUtil.isBlank(tableGroup.getSourceTable().getName())) {
                continue;
            }
            tableNames.add(tableGroup.getSourceTable().getName());
        }
        return tableNames;
    }
}
