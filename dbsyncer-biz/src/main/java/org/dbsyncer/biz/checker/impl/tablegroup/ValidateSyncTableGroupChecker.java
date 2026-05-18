/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.tablegroup;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.spi.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-03-30 20:00
 */
@Component
public class ValidateSyncTableGroupChecker extends TableGroupChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private TaskService<ValidateSyncTask> taskService;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        String taskId = params.get("taskId");
        String sourceTable = params.get("sourceTable");
        String targetTable = params.get("targetTable");
        String sourceType = params.get("sourceType");
        String targetType = params.get("targetType");
        String sourceTablePK = params.get("sourceTablePK");
        String targetTablePK = params.get("targetTablePK");
        String fieldMappings = params.get("fieldMappings");
        Assert.hasText(taskId, "tableGroup taskId is empty.");
        Assert.hasText(sourceTable, "tableGroup sourceTable is empty.");
        Assert.hasText(targetTable, "tableGroup targetTable is empty.");
        Assert.hasText(sourceType, "tableGroup sourceType is empty.");
        Assert.hasText(targetType, "tableGroup targetType is empty.");
        ValidateSyncTask task = taskService.get(taskId);
        Assert.notNull(task, "task can not be null.");

        // 检查是否存在重复映射关系
        checkRepeatedTable(profileComponent.getTableGroupAll(taskId), sourceTable, targetTable);

        // 获取连接器信息
        TableGroup tableGroup = new TableGroup();
        tableGroup.setMappingId(taskId);
        Table source = findTable(task.getSourceTable(), sourceTable, sourceType);
        Table target = findTable(task.getTargetTable(), targetTable, targetType);
        tableGroup.setSourceTable(updateTableColumn(task, ConnectorInstanceUtil.SOURCE_SUFFIX, sourceTablePK, source));
        tableGroup.setTargetTable(updateTableColumn(task, ConnectorInstanceUtil.TARGET_SUFFIX, targetTablePK, target));

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 匹配相似字段映射关系
        if (StringUtil.isNotBlank(fieldMappings)) {
            matchFieldMapping(tableGroup, fieldMappings);
        } else {
            matchFieldMapping(tableGroup);
        }

        return tableGroup;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "TableGroupChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        String fieldMappingJson = params.get("fieldMapping");
        Assert.hasText(fieldMappingJson, "TableGroupChecker check params fieldMapping is empty");

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 过滤条件
        String filterJson = params.get("filter");
        if (StringUtil.isNotBlank(filterJson)) {
            List<Filter> list = JsonUtil.jsonToArray(filterJson, Filter.class);
            tableGroup.setFilter(list);
        }

        // 字段映射关系
        setFieldMapping(tableGroup, fieldMappingJson);

        return tableGroup;
    }

    public Table updateTableColumn(ValidateSyncTask task, String suffix, String primaryKeyStr, Table table) {
        boolean isSource = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(task, isSource);
        return this.updateTableColumn(context, primaryKeyStr, table);
    }

}