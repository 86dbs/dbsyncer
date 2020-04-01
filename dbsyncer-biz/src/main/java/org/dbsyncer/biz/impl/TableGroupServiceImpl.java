package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/27 23:14
 */
@Service
public class TableGroupServiceImpl implements TableGroupService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Checker tableGroupChecker;

    @Override
    public String add(Map<String, String> params) {
        String mappingId = params.get("mappingId");
        String sourceTable = params.get("sourceTable");
        String targetTable = params.get("targetTable");
        Assert.hasText(mappingId, "tableGroup mappingId is empty.");
        Assert.hasText(sourceTable, "tableGroup sourceTable is empty.");
        Assert.hasText(targetTable, "tableGroup targetTable is empty.");
        Mapping mapping = manager.getMapping(mappingId);
        Assert.notNull(mapping, "mapping can not be null.");

        TableGroup tableGroup = new TableGroup();
        tableGroup.setMappingId(mappingId);
        // 获取表字段
        tableGroup.setSourceTable(getTable(mapping.getSourceConnectorId(), sourceTable));
        tableGroup.setTargetTable(getTable(mapping.getTargetConnectorId(), targetTable));
        String json = JsonUtil.objToJson(tableGroup);
        return manager.addTableGroup(json);
    }

    @Override
    public String edit(Map<String, String> params) {
        String json = tableGroupChecker.checkConfigModel(params);
        return manager.editTableGroup(json);
    }

    @Override
    public boolean remove(String id) {
        manager.removeTableGroup(id);
        return true;
    }

    @Override
    public TableGroup getTableGroup(String id) {
        TableGroup tableGroup = manager.getTableGroup(id);
        Assert.notNull(tableGroup, "TableGroup can not be null");
        return tableGroup;
    }

    @Override
    public List<TableGroup> getTableGroupAll(String mappingId) {
        return manager.getTableGroupAll(mappingId);
    }

    private Table getTable(String connectorId, String tableName) {
        MetaInfo metaInfo = manager.getMetaInfo(connectorId, tableName);
        Assert.notNull(metaInfo, "metaInfo can not be null.");
        return new Table().setName(tableName).setColumn(metaInfo.getColumn());
    }
}