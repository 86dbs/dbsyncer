/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.plugin.PluginCallback;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;

/**
 * @Author wuji
 * @Version 1.0.0
 * @Date 2026-04-03 15:04
 */
@Component
public class PluginCallbackImpl implements PluginCallback {


    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private BufferActuatorRouter bufferActuatorRouter;


    @Override
    public void onDataProcessed(String mappingId, String tableGroupId, String event, List<Object> data) {

        Mapping mapping = profileComponent.getMapping(mappingId);
        TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
        Assert.notNull(tableGroup, "Meta can not be null.");
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "Meta can not be null.");
        String sourceTableName = tableGroup.getSourceTable().getName();
        RowChangedEvent changedEvent = new RowChangedEvent(sourceTableName, event, data, null, null);

        // 执行同步是否成功
        bufferActuatorRouter.execute(meta.getId(), changedEvent);
    }
}
