/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.consumer;

import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.flush.impl.BufferActuatorRouter;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.listener.ChangedEvent;
import org.dbsyncer.sdk.listener.Watcher;
import org.dbsyncer.sdk.listener.event.DDLChangedEvent;
import org.dbsyncer.sdk.listener.event.SqlChangedEvent;

import java.util.List;
import java.util.Map;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public abstract class AbstractConsumer<E extends ChangedEvent> implements Watcher {
    private BufferActuatorRouter bufferActuatorRouter;
    private ProfileComponent profileComponent;
    private LogService logService;
    private String metaId;
    protected Mapping mapping;
    protected List<TableGroup> tableGroups;

    public AbstractConsumer init(BufferActuatorRouter bufferActuatorRouter, ProfileComponent profileComponent, LogService logService, String metaId, Mapping mapping, List<TableGroup> tableGroups) {
        this.bufferActuatorRouter = bufferActuatorRouter;
        this.profileComponent = profileComponent;
        this.logService = logService;
        this.metaId = metaId;
        this.mapping = mapping;
        this.tableGroups = tableGroups;
        postProcessBeforeInitialization();
        return this;
    }

    public abstract void postProcessBeforeInitialization();

    public abstract void onChange(E e);

    public void onDDLChanged(DDLChangedEvent event) {
    }

    public void onSqlChanged(SqlChangedEvent event) {
    }

    @Override
    public void changeEvent(ChangedEvent event) {
        event.getChangedOffset().setMetaId(metaId);
        switch (event.getType()){
            case ROW:
            case SCAN:
                onChange((E) event);
                break;
            case SQL:
                onSqlChanged((SqlChangedEvent) event);
                break;
            case DDL:
                onDDLChanged((DDLChangedEvent) event);
                break;
        }
    }

    @Override
    public void flushEvent(Map<String, String> snapshot) {
        Meta meta = profileComponent.getMeta(metaId);
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
        Meta meta = profileComponent.getMeta(metaId);
        return meta != null ? meta.getUpdateTime() : 0L;
    }

    protected void bind(String tableGroupId) {
        bufferActuatorRouter.bind(metaId, tableGroupId);
    }

    protected void execute(String tableGroupId, ChangedEvent event) {
        bufferActuatorRouter.execute(metaId, tableGroupId, event);
    }
}