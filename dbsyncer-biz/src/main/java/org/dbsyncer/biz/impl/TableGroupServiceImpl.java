/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker;
import org.dbsyncer.biz.task.TableGroupCountTask;
import org.dbsyncer.common.dispatch.DispatchTaskService;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.rsa.RsaManager;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.model.Field;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private RsaManager rsaManager;

    @Resource
    private DispatchTaskService dispatchTaskService;

    @Override
    public String add(Map<String, String> params) {
        String mappingId = params.get("mappingId");
        Mapping mapping = profileComponent.getMapping(mappingId);
        assertRunning(mapping);

        synchronized (LOCK) {
            // table1, table2
            String[] sourceTableArray = StringUtil.split(params.get("sourceTable"), StringUtil.VERTICAL_LINE);
            String[] targetTableArray = StringUtil.split(params.get("targetTable"), StringUtil.VERTICAL_LINE);
            int tableSize = sourceTableArray.length;
            Assert.isTrue(tableSize == targetTableArray.length, "数据源表和目标源表关系必须为一组");

            int baseIndex = tableGroupProfile.getTableGroupCount(mappingId);
            params.put("skipRepeatedCheck", Boolean.TRUE.toString());
            List<TableGroup> models = new ArrayList<>(tableSize);
            for (int i = 0; i < tableSize; i++) {
                params.put("sourceTable", sourceTableArray[i]);
                params.put("targetTable", targetTableArray[i]);
                TableGroup model = (TableGroup) tableGroupChecker.checkAddConfigModel(params);
                log(LogType.TableGroupLog.INSERT, model);
                model.setIndex(baseIndex + i + 1);
                models.add(model);
            }
            tableGroupProfile.addTableGroupBatch(models);

            List<String> list = new ArrayList<>(models.size());
            for (TableGroup model : models) {
                list.add(model.getId());
            }
            submitTableGroupCountTask(mapping, list);

            // 合并驱动公共字段
            mergeMappingColumn(mapping);
            return 1 < tableSize ? String.valueOf(tableSize) : list.get(0);
        }
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = tableGroupProfile.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        Mapping mapping = profileComponent.getMapping(tableGroup.getTaskId());
        assertRunning(mapping);

        TableGroup model = (TableGroup) tableGroupChecker.checkEditConfigModel(params);
        log(LogType.TableGroupLog.UPDATE, model);
        tableGroupProfile.editTableGroup(model);
        List<String> list = new ArrayList<>();
        list.add(model.getId());
        submitTableGroupCountTask(mapping, list);
        return id;
    }

    @Override
    public String refreshFields(String id) {
        TableGroup tableGroup = tableGroupProfile.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");

        tableGroupChecker.refreshTableFields(tableGroup);
        return tableGroupProfile.editTableGroup(tableGroup);
    }

    @Override
    public String remove(String mappingId, String ids) {
        Assert.hasText(mappingId, "Mapping id can not be null");
        Assert.hasText(ids, "TableGroup ids can not be null");
        Mapping mapping = profileComponent.getMapping(mappingId);
        assertRunning(mapping);

        // 批量删除表
        Stream.of(StringUtil.split(ids, ",")).parallel().forEach(id-> {
            TableGroup model = tableGroupProfile.getTableGroup(id);
            log(LogType.TableGroupLog.DELETE, model);
            tableGroupProfile.removeTableGroup(id);
        });

        // 合并驱动公共字段
        mergeMappingColumn(mapping);
        submitTableGroupCountTask(mapping, Collections.emptyList());

        // 重置排序
        resetTableGroupAllIndex(mappingId);
        return mappingId;
    }

    @Override
    public TableGroup getTableGroup(String id) {
        TableGroup tableGroup = tableGroupProfile.getTableGroup(id);
        Assert.notNull(tableGroup, "TableGroup can not be null");
        return tableGroup;
    }

    @Override
    public Paging<TableGroup> search(Map<String, String> params) {
        String mappingId = params.get("mappingId");
        Assert.hasText(mappingId, "Mapping id can not be null");
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        return tableGroupProfile.queryTableGroup(mappingId, params.get("searchKey"), pageNum, pageSize);
    }

    @Override
    public Meta updateMeta(Mapping mapping, String metaSnapshot) {
        Meta meta = metaProfile.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "驱动meta不存在.");

        // 清空状态
        meta.clear();

        // 手动配置增量点
        if (StringUtil.isNotBlank(metaSnapshot)) {
            Map snapshot = JsonUtil.jsonToObj(metaSnapshot, HashMap.class);
            if (!CollectionUtils.isEmpty(snapshot)) {
                meta.setSnapshot(snapshot);
            }
        }

        getMetaTotal(meta, mapping.getModel());

        meta.setUpdateTime(Instant.now().toEpochMilli());
        profileComponent.editConfigModel(meta);
        return meta;
    }

    private void getMetaTotal(Meta meta, String model) {
        // 全量同步
        if (ModelEnum.isFull(model)) {
            // 统计tableGroup总条数
            AtomicLong count = new AtomicLong(0);
            tableGroupProfile.pageScanTableGroups(meta.getTaskId(), ConfigConstant.PAGE_SIZE, groupAll -> {
                for (TableGroup g : groupAll) {
                    if (g != null && g.getSourceTable() != null) {
                        count.getAndAdd(g.getSourceTable().getCount());
                    }
                }
            });
            meta.setTotal(count);
        }
    }

    private void resetTableGroupAllIndex(String mappingId) {
        synchronized (LOCK) {
            List<String> orderedIds = new ArrayList<>();
            tableGroupProfile.pageScanTableGroups(mappingId, ConfigConstant.PAGE_SIZE, page -> {
                for (TableGroup g : page) {
                    if (g != null && StringUtil.isNotBlank(g.getId())) {
                        orderedIds.add(g.getId());
                    }
                }
            });
            int i = orderedIds.size();
            for (String id : orderedIds) {
                TableGroup g = tableGroupProfile.getTableGroup(id);
                if (g == null) {
                    continue;
                }
                g.setIndex(i--);
                profileComponent.editConfigModel(g);
            }
        }
    }

    private void mergeMappingColumn(Mapping mapping) {
        List<Field> sourceColumn = null;
        List<Field> targetColumn = null;
        final List<Field>[] holder = new List[]{sourceColumn, targetColumn};
        tableGroupProfile.pageScanTableGroups(mapping.getId(), ConfigConstant.PAGE_SIZE, groups -> {
            for (TableGroup g : groups) {
                holder[0] = PickerUtil.pickCommonFields(holder[0], g.getSourceTable().getColumn());
                holder[1] = PickerUtil.pickCommonFields(holder[1], g.getTargetTable().getColumn());
            }
        });
        mapping.setSourceColumn(holder[0]);
        mapping.setTargetColumn(holder[1]);
        profileComponent.editConfigModel(mapping);
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
        task.setTableGroupProfile(tableGroupProfile);
        task.setConnectorFactory(connectorFactory);
        task.setRsaManager(rsaManager);
        task.setTableGroupService(this);
        dispatchTaskService.execute(task);
    }

}