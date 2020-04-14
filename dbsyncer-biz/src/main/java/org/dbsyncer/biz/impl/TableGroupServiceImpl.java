package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.*;

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
        TableGroup model = (TableGroup) tableGroupChecker.checkAddConfigModel(params);
        // 匹配相似字段
        mergeTableGroupColumn(model);

        String id = manager.addTableGroup(model);

        // 合并驱动公共字段
        mergeMappingColumn(model.getMappingId());
        return id;
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = tableGroupChecker.checkEditConfigModel(params);
        return manager.editTableGroup(model);
    }

    @Override
    public boolean remove(String id) {
        TableGroup tableGroup = manager.getTableGroup(id);
        Assert.notNull(tableGroup, "tableGroup can not be null.");

        manager.removeTableGroup(id);
        mergeMappingColumn(tableGroup.getMappingId());
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

    private void mergeMappingColumn(String mappingId) {
        List<TableGroup> groups = manager.getTableGroupAll(mappingId);

        Mapping mapping = manager.getMapping(mappingId);
        Assert.notNull(mapping, "mapping not exist.");

        List<Field> sourceColumn = null;
        List<Field> targetColumn = null;
        for (TableGroup g : groups) {
            sourceColumn = pickCommonFields(sourceColumn, g.getSourceTable().getColumn());
            targetColumn = pickCommonFields(targetColumn, g.getTargetTable().getColumn());
        }

        mapping.setSourceColumn(sourceColumn);
        mapping.setTargetColumn(targetColumn);
        manager.editMapping(mapping);
    }

    private List<Field> pickCommonFields(List<Field> column, List<Field> target) {
        if (CollectionUtils.isEmpty(column) || CollectionUtils.isEmpty(target)) {
            return target;
        }
        List<Field> list = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        column.forEach(f -> keys.add(f.getName()));
        target.forEach(f -> {
            if (keys.contains(f.getName())) {
                list.add(f);
            }
        });
        return list;
    }

    private void mergeTableGroupColumn(TableGroup tableGroup) {
        List<Field> sCol = tableGroup.getSourceTable().getColumn();
        List<Field> tCol = tableGroup.getTargetTable().getColumn();
        if (CollectionUtils.isEmpty(sCol) || CollectionUtils.isEmpty(tCol)) {
            return;
        }

        // Set集合去重
        Map<String, Field> m1 = new HashMap<>();
        Map<String, Field> m2 = new HashMap<>();
        List<String> k1 = new LinkedList<>();
        List<String> k2 = new LinkedList<>();
        shuffleColumn(sCol, k1, m1);
        shuffleColumn(tCol, k2, m2);
        k1.retainAll(k2);

        // 有相似字段
        if (!CollectionUtils.isEmpty(k1)) {
            List<FieldMapping> fields = new ArrayList<>();
            k1.forEach(k -> fields.add(new FieldMapping(m1.get(k), m2.get(k))));
            tableGroup.setFieldMapping(fields);
        }
    }

    private void shuffleColumn(List<Field> col, List<String> key, Map<String, Field> map) {
        col.forEach(f -> {
            if (!key.contains(f.getName())) {
                key.add(f.getName());
                map.put(f.getName(), f);
            }
        });
    }

}