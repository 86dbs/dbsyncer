/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.consumer;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.QuartzListenerContext;
import org.dbsyncer.sdk.listener.Watcher;
import org.dbsyncer.sdk.spi.BufferActuatorRouterService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public final class ParserConsumer implements Watcher {

    private final BufferActuatorRouterService bufferActuatorRouter;
    private final MetaProfile metaProfile;
    private final ProfileComponent profileComponent;
    private final PluginFactory pluginFactory;
    private final LogService logService;
    private final String metaId;

    public ParserConsumer(BufferActuatorRouterService bufferActuatorRouter, MetaProfile metaProfile, ProfileComponent profileComponent,
                          PluginFactory pluginFactory, LogService logService, String metaId,
                          List<TableGroup> tableGroups, int channelSize) {
        this.bufferActuatorRouter = bufferActuatorRouter;
        this.metaProfile = metaProfile;
        this.profileComponent = profileComponent;
        this.pluginFactory = pluginFactory;
        this.logService = logService;
        this.metaId = metaId;
        bufferActuatorRouter.bind(metaId, extractSourceTableNames(tableGroups), channelSize);
    }

    @Override
    public void changeEventBefore(QuartzListenerContext context) {
        pluginFactory.process(context, ProcessEnum.BEFORE);
    }

    @Override
    public void changeEvent(ChangedEvent event) {
        bufferActuatorRouter.execute(metaId, event);
    }

    @Override
    public void flushEvent(Map<String, String> snapshot) {
        Meta meta = metaProfile.getMeta(metaId);
        if (meta != null) {
            meta.setSnapshot(snapshot);
            profileComponent.editConfigModel(meta);
        }
    }

    @Override
    public void errorEvent(Exception e) {
        logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
    }

    @Override
    public long getMetaUpdateTime() {
        Meta meta = metaProfile.getMeta(metaId);
        return meta != null ? meta.getUpdateTime() : 0L;
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
