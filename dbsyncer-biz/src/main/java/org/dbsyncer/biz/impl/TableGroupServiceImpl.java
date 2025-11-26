/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker;
import org.dbsyncer.biz.task.TableGroupCountTask;
import org.dbsyncer.common.dispatch.DispatchTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.Field;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/27 23:14
 */
@Service
public class TableGroupServiceImpl extends BaseServiceImpl implements TableGroupService {

    @Resource
    private TableGroupChecker tableGroupChecker;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private DispatchTaskService dispatchTaskService;

    @Override
    public String add(Map<String, String> params) throws Exception {
        String mappingId = params.get("mappingId");
        Mapping mapping = profileComponent.getMapping(mappingId);
        assertRunning(mapping);

        synchronized (LOCK) {
            // table1, table2
            String[] sourceTableArray = StringUtil.split(params.get("sourceTable"), StringUtil.VERTICAL_LINE);
            String[] targetTableArray = StringUtil.split(params.get("targetTable"), StringUtil.VERTICAL_LINE);
            int tableSize = sourceTableArray.length;
            Assert.isTrue(tableSize == targetTableArray.length, "数据源表和目标源表关系必须为一组");

            String id = null;
            List<String> list = new ArrayList<>();
            for (int i = 0; i < tableSize; i++) {
                params.put("sourceTable", sourceTableArray[i]);
                params.put("targetTable", targetTableArray[i]);
                TableGroup model = (TableGroup) tableGroupChecker.checkAddConfigModel(params);
                log(LogType.TableGroupLog.INSERT, model);
                int tableGroupCount = profileComponent.getTableGroupCount(mappingId);
                model.setIndex(tableGroupCount + 1);
                id = profileComponent.addTableGroup(model);
                list.add(id);
            }
            submitTableGroupCountTask(mapping, list);

            // 合并驱动公共字段
            mergeMappingColumn(mapping);
            return 1 < tableSize ? String.valueOf(tableSize) : id;
        }
    }

    @Override
    public String edit(Map<String, String> params) throws Exception {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
        assertRunning(mapping);

        TableGroup model = (TableGroup) tableGroupChecker.checkEditConfigModel(params);
        log(LogType.TableGroupLog.UPDATE, model);
        profileComponent.editTableGroup(model);
        List<String> list = new ArrayList<>();
        list.add(model.getId());
        submitTableGroupCountTask(mapping, list);
        return id;
    }

    @Override
    public String refreshFields(String id) throws Exception {
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");

        tableGroupChecker.refreshTableFields(tableGroup);
        return profileComponent.editTableGroup(tableGroup);
    }

    @Override
    public boolean remove(String mappingId, String ids) throws Exception {
        Assert.hasText(mappingId, "Mapping id can not be null");
        Assert.hasText(ids, "TableGroup ids can not be null");
        Mapping mapping = profileComponent.getMapping(mappingId);
        assertRunning(mapping);

        // 批量删除表
        Stream.of(StringUtil.split(ids, ",")).parallel().forEach(id -> {
            TableGroup model = null;
            try {
                model = profileComponent.getTableGroup(id);
                log(LogType.TableGroupLog.DELETE, model);
                profileComponent.removeTableGroup(id);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // 合并驱动公共字段
        mergeMappingColumn(mapping);
        submitTableGroupCountTask(mapping, Collections.emptyList());

        // 重置排序
        resetTableGroupAllIndex(mappingId);
        return true;
    }

    @Override
    public TableGroup getTableGroup(String id) throws Exception {
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "TableGroup can not be null");
        return tableGroup;
    }

    @Override
    public List<TableGroup> getTableGroupAll(String mappingId) throws Exception {
        return profileComponent.getSortedTableGroupAll(mappingId);
    }

    private void resetTableGroupAllIndex(String mappingId) throws Exception {
        synchronized (LOCK) {
            List<TableGroup> list = profileComponent.getSortedTableGroupAll(mappingId);
            int size = list.size();
            int i = size;
            while (i > 0) {
                TableGroup g = list.get(size - i);
                g.setIndex(i);
                profileComponent.editConfigModel(g);
                i--;
            }
        }
    }

    private void mergeMappingColumn(Mapping mapping) throws Exception {
        List<TableGroup> groups = profileComponent.getTableGroupAll(mapping.getId());

        List<Field> sourceColumn = null;
        List<Field> targetColumn = null;
        for (TableGroup g : groups) {
            sourceColumn = pickCommonFields(sourceColumn, g.getSourceTable().getColumn());
            targetColumn = pickCommonFields(targetColumn, g.getTargetTable().getColumn());
        }

        mapping.setSourceColumn(sourceColumn);
        mapping.setTargetColumn(targetColumn);
        profileComponent.editConfigModel(mapping);
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

    /**
     * 提交统计驱动表总数任务
     */
    private void submitTableGroupCountTask(Mapping mapping, List<String> list) {
        TableGroupCountTask task = new TableGroupCountTask();
        task.setMappingId(mapping.getId());
        task.setTableGroups(list);
        task.setParserComponent(parserComponent);
        task.setProfileComponent(profileComponent);
        task.setTableGroupService(this);
        dispatchTaskService.execute(task);
    }

}