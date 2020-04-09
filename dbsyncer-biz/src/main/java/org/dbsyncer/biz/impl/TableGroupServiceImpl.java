package org.dbsyncer.biz.impl;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
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

        // 检查是否存在重复映射关系
        checkRepeatedTable(mappingId, sourceTable, targetTable);

        // 读取表信息
        TableGroup tableGroup = new TableGroup();
        tableGroup.setMappingId(mappingId);
        Table sTable = getTable(mapping.getSourceConnectorId(), sourceTable);
        Table tTable = getTable(mapping.getTargetConnectorId(), targetTable);
        tableGroup.setSourceTable(sTable);
        tableGroup.setTargetTable(tTable);

        // 合并驱动公共字段
        mergeMappingColumn(mapping, sTable.getColumn(), tTable.getColumn());

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
        TableGroup tableGroup = manager.getTableGroup(id);
        Assert.notNull(tableGroup, "tableGroup can not be null.");

        manager.removeTableGroup(id);
        Mapping mapping = manager.getMapping(tableGroup.getMappingId());
        Assert.notNull(mapping, "mapping not exist.");

        // 合并驱动公共字段
        Table sTable = tableGroup.getSourceTable();
        Table tTable = tableGroup.getTargetTable();
        mergeMappingColumn(mapping, sTable.getColumn(), tTable.getColumn());

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
        Assert.notNull(metaInfo, "无法获取连接信息.");
        return new Table().setName(tableName).setColumn(metaInfo.getColumn());
    }

    private void checkRepeatedTable(String mappingId, String sourceTable, String targetTable) {
        List<TableGroup> list = manager.getTableGroupAll(mappingId);
        if (!CollectionUtils.isEmpty(list)) {
            for (TableGroup g : list) {
                // 数据源表和目标表都存在
                if(StringUtils.equals(sourceTable, g.getSourceTable().getName()) && StringUtils.equals(targetTable, g.getTargetTable().getName())){
                    final String error = String.format("映射关系[%s]>>[%s]已存在.", sourceTable, targetTable);
                    logger.error(error);
                    throw new BizException(error);
                }
            }
        }
    }

    private void mergeMappingColumn(Mapping mapping, List<Field> sColumn, List<Field> tColumn) {
        mapping.setSourceColumn(pickCommonFields(mapping.getSourceColumn(), sColumn));
        mapping.setTargetColumn(pickCommonFields(mapping.getTargetColumn(), tColumn));
        String json = JsonUtil.objToJson(mapping);
        manager.editMapping(json);
    }

    private List<Field> pickCommonFields(List<Field> column, List<Field> target) {
        if(CollectionUtils.isEmpty(column) || CollectionUtils.isEmpty(target)){
            return target;
        }
        List<Field> list = new ArrayList<>();
        Map<String, Boolean> map = new HashMap<>(column.size());
        column.forEach(f -> map.putIfAbsent(f.getName(), true));
        target.forEach(f -> {
            if(map.get(f.getName())){
                list.add(f);
            }
        });
        return list;
    }

}