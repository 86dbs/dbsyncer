/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.task;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.common.dispatch.AbstractDispatchTask;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.rsa.RsaManager;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultMetaContext;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.MetaIncrement;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.Map;

/**
 * 抽象类统计驱动总数任务
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-06-25 01:00
 */
public abstract class AbstractCountTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected String mappingId;

    protected ParserComponent parserComponent;

    protected SystemConfigProfile systemConfigProfile;

    protected ConnectorProfile connectorProfile;

    protected TaskProfile taskProfile;

    protected TableGroupProfile tableGroupProfile;

    protected TableGroupService tableGroupService;

    protected ConnectorFactory connectorFactory;

    protected MetaProfile metaProfile;

    private RsaManager rsaManager;

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public void setParserComponent(ParserComponent parserComponent) {
        this.parserComponent = parserComponent;
    }

    public void setSystemConfigProfile(SystemConfigProfile systemConfigProfile) {
        this.systemConfigProfile = systemConfigProfile;
    }

    public void setConnectorProfile(ConnectorProfile connectorProfile) {
        this.connectorProfile = connectorProfile;
    }

    public void setTaskProfile(TaskProfile taskProfile) {
        this.taskProfile = taskProfile;
    }

    public void setTableGroupProfile(TableGroupProfile tableGroupProfile) {
        this.tableGroupProfile = tableGroupProfile;
    }

    public void setTableGroupService(TableGroupService tableGroupService) {
        this.tableGroupService = tableGroupService;
    }

    public void setConnectorFactory(ConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setRsaManager(RsaManager rsaManager) {
        this.rsaManager = rsaManager;
    }

    public void setMetaProfile(MetaProfile metaProfile) {
        this.metaProfile = metaProfile;
    }

    protected void updateTableGroupCount(Mapping mapping, TableGroup tableGroup) {
        long now = Instant.now().toEpochMilli();
        TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        Map<String, String> command = parserComponent.getCommand(mapping, group);
        String sourceConnectorId = mapping.getSourceConnectorId();
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mappingId, sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        ConnectorConfig config = connectorProfile.getConnector(sourceConnectorId).getConfig();
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        Assert.notNull(command, "command can not null");
        ConnectorService connectorService = connectorFactory.getConnectorService(config);

        DefaultMetaContext metaContext = new DefaultMetaContext();
        metaContext.setCommand(command);
        metaContext.setSourceTable(group.getSourceTable());
        metaContext.setSourceConnectorInstance(connectorInstance);
        setRsaConfig(metaContext);

        long count = connectorService.getCount(connectorInstance, metaContext);
        // SOURCE_TOTAL 列与 JSON 内 sourceTable.count 同步，供明细查询 / 任务 Meta 汇总
        tableGroup.setSourceTotal(count);
        tableGroup.getSourceTable().setCount(count);
        tableGroupProfile.editTableGroup(tableGroup);
        syncTableDetailMetaTotal(tableGroup.getId(), count);
        logger.info("{}表{}, 总数:{}, {}ms", mapping.getName(), tableGroup.getSourceTable().getName(), count, (Instant.now().toEpochMilli() - now));
    }

    /**
     * 将表级明细 Meta.TOTAL 对齐到源表统计值（原子增量，避免整行覆盖 success/fail）。
     */
    private void syncTableDetailMetaTotal(String tableGroupId, long count) {
        if (metaProfile == null || StringUtil.isBlank(tableGroupId)) {
            return;
        }
        Meta tableMeta = metaProfile.getMetaByTaskId(tableGroupId, TaskLevelEnum.TASK_DETAIL);
        if (tableMeta == null || StringUtil.isBlank(tableMeta.getId())) {
            return;
        }
        long oldTotal = tableMeta.getTotal() == null ? 0L : tableMeta.getTotal().get();
        long delta = count - oldTotal;
        if (delta == 0L) {
            return;
        }
        metaProfile.incrementMeta(MetaIncrement.of(tableMeta.getId()).total(delta));
    }

    protected boolean shouldStop(Mapping mapping) {
        return !isRunning() || !ModelEnum.isFull(mapping.getModel());
    }

    private void setRsaConfig(DefaultMetaContext context) {
        SystemConfig systemConfig = systemConfigProfile.getSystemConfig();
        if (systemConfig.isEnableOpenAPI()) {
            context.setRsaManager(rsaManager);
            context.setRsaConfig(systemConfig.getRsaConfig());
        }
    }
}
