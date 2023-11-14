package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/27 23:14
 */
@Service
public class TableGroupServiceImpl extends BaseServiceImpl implements TableGroupService {

    @Resource
    private Checker tableGroupChecker;

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public String add(Map<String, String> params) {
        String mappingId = params.get("mappingId");
        assertRunning(profileComponent.getMapping(mappingId));

        synchronized (LOCK) {
            // table1, table2
            String[] sourceTableArray = StringUtil.split(params.get("sourceTable"), "|");
            String[] targetTableArray = StringUtil.split(params.get("targetTable"), "|");
            int tableSize = sourceTableArray.length;
            Assert.isTrue(tableSize == targetTableArray.length, "数据源表和目标源表关系必须为一组");

            String id = null;
            for (int i = 0; i < tableSize; i++) {
                params.put("sourceTable", sourceTableArray[i]);
                params.put("targetTable", targetTableArray[i]);
                TableGroup model = (TableGroup) tableGroupChecker.checkAddConfigModel(params);
                log(LogType.TableGroupLog.INSERT, model);
                int tableGroupCount = profileComponent.getTableGroupCount(mappingId);
                model.setIndex(tableGroupCount + 1);
                id = profileComponent.addTableGroup(model);
            }

            // 合并驱动公共字段
            mergeMappingColumn(mappingId);
            return 1 < tableSize ? String.valueOf(tableSize) : id;
        }
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        assertRunning(profileComponent.getMapping(tableGroup.getMappingId()));

        TableGroup model = (TableGroup) tableGroupChecker.checkEditConfigModel(params);
        log(LogType.TableGroupLog.UPDATE, model);

        return profileComponent.editTableGroup(model);
    }

    @Override
    public String refreshFields(String id) {
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        assertRunning(profileComponent.getMapping(tableGroup.getMappingId()));

        TableGroupChecker checker = (TableGroupChecker) tableGroupChecker;
        checker.refreshTableFields(tableGroup);

        return profileComponent.editTableGroup(tableGroup);
    }

    @Override
    public boolean remove(String mappingId, String ids) {
        Assert.hasText(mappingId, "Mapping id can not be null");
        Assert.hasText(ids, "TableGroup ids can not be null");
        assertRunning(profileComponent.getMapping(mappingId));

        // 批量删除表
        Stream.of(StringUtil.split(ids, ",")).parallel().forEach(id -> {
            TableGroup model = profileComponent.getTableGroup(id);
            log(LogType.TableGroupLog.DELETE, model);
            profileComponent.removeTableGroup(id);
        });

        // 合并驱动公共字段
        mergeMappingColumn(mappingId);

        // 重置排序
        resetTableGroupAllIndex(mappingId);
        return true;
    }

    @Override
    public TableGroup getTableGroup(String id) {
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "TableGroup can not be null");
        return tableGroup;
    }

    @Override
    public List<TableGroup> getTableGroupAll(String mappingId) {
        return profileComponent.getSortedTableGroupAll(mappingId);
    }

    private void resetTableGroupAllIndex(String mappingId) {
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

    private void mergeMappingColumn(String mappingId) {
        List<TableGroup> groups = profileComponent.getTableGroupAll(mappingId);

        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "mapping not exist.");

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

}