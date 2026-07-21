/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.DatabaseSyncService;
import org.dbsyncer.biz.vo.DatabaseSyncTaskVO;
import org.dbsyncer.biz.vo.TablePreviewVO;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.enums.CommonTaskStepStatusEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.DatabaseSyncProcessor;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.TableMapping;
import org.dbsyncer.sdk.spi.DatabaseSyncDetailService;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 整库迁移业务实现
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 00:00
 */
@Service
public class DatabaseSyncServiceImpl implements DatabaseSyncService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private TaskService<DatabaseSyncTask> taskService;

    @Resource
    private StorageService storageService;

    @Resource
    private DatabaseSyncDetailService databaseSyncDetailService;

    @Override
    public DatabaseSyncTaskVO get(String id) {
        DatabaseSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        return convertTask2Vo(task);
    }

    @Override
    public String add(Map<String, String> params) {
        String name = params.get("name");
        if (StringUtil.isBlank(name)) {
            throw new BizException("任务名称不能为空");
        }
        List<DatabaseMapping> mappings = parseDatabaseMappings(params.get("databaseMappingsJson"));
        checkDatabaseMapping(mappings);
        if (CollectionUtils.isEmpty(mappings)) {
            throw new BizException("请至少添加一组库映射");
        }
        normalizeAndSortMappings(mappings);
        validateMappingConnectors(mappings);

        DatabaseSyncTask task = new DatabaseSyncTask();
        fillTaskOnAdd(task, params);
        // 先落任务与任务级 Meta，再写 table_group，避免 task 失败留下孤儿关联
        String taskId = taskService.add(task);
        persistTableGroupsFromMappings(taskId, mappings);
        logger.info("整库迁移任务已保存: id={}, name={}, mappingCount={}", taskId, name, mappings.size());
        return taskId;
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get("id");
        Assert.hasText(id, "任务 ID 不能为空");
        DatabaseSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        assertNotRunning(id);

        List<DatabaseMapping> mappings = parseDatabaseMappings(params.get("databaseMappingsJson"));
        checkDatabaseMapping(mappings);
        if (CollectionUtils.isEmpty(mappings)) {
            throw new BizException("请至少添加一组库映射");
        }
        normalizeAndSortMappings(mappings);
        validateMappingConnectors(mappings);

        fillTaskOnEdit(task, params);
        clearTableGroups(task.getId());
        persistTableGroupsFromMappings(id, mappings);
        task.getDatabaseSnapshots().clear();
        task.setProcessed(CommonTaskStepStatusEnum.PENDING.getCode());
        return taskService.edit(task);
    }

    private  void checkDatabaseMapping(List<DatabaseMapping> mappings) {
        for (DatabaseMapping mapping : mappings) {
            if (StringUtil.equals(mapping.getSourceConnectorId(), mapping.getTargetConnectorId())) {
                boolean selectedDB = StringUtil.isNotBlank(mapping.getSourceDatabase()) && StringUtil.isNotBlank(mapping.getTargetDatabase());
                if (selectedDB && StringUtil.equals(mapping.getSourceDatabase(), mapping.getTargetDatabase())) {
                    throw new BizException("同源同库不允许同步，请更换目标连接或数据库！");
                }
                boolean selectedSchema = StringUtil.isNotBlank(mapping.getSourceSchema()) && StringUtil.isNotBlank(mapping.getTargetSchema());
                if (!selectedDB && selectedSchema && StringUtil.equals(mapping.getSourceSchema(), mapping.getTargetSchema())) {
                    throw new BizException("同源同schema不允许同步，请更换目标连接或schema！");
                }

            }
        }
    }

    @Override
    public String delete(String id) {
        Assert.hasText(id, "任务 ID 不能为空");
        assertNotRunning(id);
        taskService.delete(id);
        return "删除成功";
    }

    @Override
    public String start(String id) {
        Assert.hasText(id, "任务 ID 不能为空");
        DatabaseSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        if (profileComponent.getTableGroupCount(id) <= 0) {
            throw new BizException("任务未配置库表映射，无法启动");
        }
        taskService.start(id);
        return "启动成功";
    }

    @Override
    public String stop(String id) {
        Assert.hasText(id, "任务 ID 不能为空");
        taskService.stop(id);
        return "停止成功";
    }

    @Override
    public Paging<DatabaseSyncTaskVO> search(Map<String, String> params) {
        Paging paging = taskService.search(params, CommonTaskTypeEnum.DATABASE_SYNC);
        Collection data = paging.getData();
        if (CollectionUtils.isEmpty(data)) {
            return paging;
        }
        List<DatabaseSyncTaskVO> list = new ArrayList<>();
        data.forEach(item -> {
            if (item instanceof DatabaseSyncTask) {
                DatabaseSyncTask task = (DatabaseSyncTask) item;
                DatabaseSyncTaskVO vo = convertTask2Vo(task);
                if (vo != null) {
                    int tableCount = profileComponent.getTableGroupCount(task.getId());
                    vo.setProgress(DatabaseSyncProcessor.calculateProgressPercent(vo, tableCount, vo.getMappingCount()));
                    vo.setTotalTableCount(tableCount);
                    vo.setCompletedTableCount(DatabaseSyncProcessor.countCompletedTables(vo, tableCount));
                    vo.setErrorCount(profileComponent.countTaskDetailBySuccess(task.getId(), 0));
                    list.add(vo);
                }
            }
        });
        paging.setData(list);
        return paging;
    }

    @Override
    public List<DatabaseSyncTaskVO> getAll() {
        return taskService.getTaskAll(CommonTaskTypeEnum.DATABASE_SYNC).stream()
                .filter(DatabaseSyncTask.class::isInstance)
                .map(t -> convertTask2Vo((DatabaseSyncTask) t))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public Paging searchResult(Map<String, String> params) {
        return databaseSyncDetailService.result(params);
    }

    @Override
    public TablePreviewVO previewTables(Map<String, String> params) {
        String connectorId = params.get("connectorId");
        String database = params.get("database");
        String schema = params.get("schema");
        String searchKey = StringUtil.trimToEmpty(params.get("searchKey"));
        int offset = Math.max(0, NumberUtil.toInt(params.get("offset"), 0));
        int limit = NumberUtil.toInt(params.get("limit"), 0);
        if (limit <= 0) {
            throw new BizException("limit 必须大于 0");
        }
        if (StringUtil.isBlank(connectorId)) {
            throw new BizException("连接器不能为空");
        }

        Connector connector = profileComponent.getConnector(connectorId);
        if (connector == null) {
            throw new BizException("连接器不存在");
        }

        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(
                "database-sync-preview",
                connectorId, database, schema,
                connectorId, database, schema,
                true);
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId());
        List<Table> tables = connectorFactory.getTables(connectorInstance, context);
        if (CollectionUtils.isEmpty(tables)) {
            return TablePreviewVO.of(Collections.emptyList(), 0, String.valueOf(offset), limit);
        }

        if (StringUtil.isNotBlank(searchKey)) {
            String key = searchKey.toUpperCase();
            tables = tables.stream()
                    .filter(t -> t.getName() != null && t.getName().toUpperCase().contains(key))
                    .collect(Collectors.toList());
        }
        // 追加表映射时排除已添加源表，避免分页偏移在排除后错位并触发前端连续空翻页
        Set<String> excludeTables = parseExcludeTables(params.get("excludeTablesJson"));
        if (!CollectionUtils.isEmpty(excludeTables)) {
            tables = tables.stream()
                    .filter(t -> t.getName() == null || !excludeTables.contains(t.getName()))
                    .collect(Collectors.toList());
        }
        tables.sort(Comparator.comparing(Table::getName, String.CASE_INSENSITIVE_ORDER));

        int realTotal = tables.size();
        Map<String, Integer> typeCounts = new HashMap<>(4);
        for (Table table : tables) {
            String type = table.getType() != null ? table.getType() : TableTypeEnum.TABLE.getCode();
            typeCounts.merge(type.toUpperCase(), 1, Integer::sum);
        }

        int from = Math.min(offset, realTotal);
        int to = Math.min(from + limit, realTotal);
        List<Map<String, Object>> pageRows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            Table table = tables.get(i);
            Map<String, Object> row = new HashMap<>(4);
            row.put("name", table.getName());
            row.put("type", table.getType() != null ? table.getType() : TableTypeEnum.TABLE.getCode());
            pageRows.add(row);
        }

        TablePreviewVO result = TablePreviewVO.of(pageRows, realTotal, String.valueOf(offset), limit);
        result.setTypeCounts(typeCounts);
        return result;
    }

    private List<DatabaseMapping> parseDatabaseMappings(String mappingsJson) {
        if (StringUtil.isBlank(mappingsJson)) {
            return Collections.emptyList();
        }
        List<DatabaseMapping> mappings = JsonUtil.jsonToArray(mappingsJson, DatabaseMapping.class);
        return mappings == null ? Collections.emptyList() : mappings;
    }

    private Set<String> parseExcludeTables(String excludeTablesJson) {
        if (StringUtil.isBlank(excludeTablesJson)) {
            return Collections.emptySet();
        }
        List<String> names = JsonUtil.jsonToArray(excludeTablesJson, String.class);
        if (CollectionUtils.isEmpty(names)) {
            return Collections.emptySet();
        }
        Set<String> exclude = new HashSet<>();
        for (String name : names) {
            if (StringUtil.isNotBlank(name)) {
                exclude.add(name);
            }
        }
        return exclude;
    }

    /**
     * 按序号从小到大排序，并规范为连续序号 1..n，便于任务执行与恢复。
     */
    private void normalizeAndSortMappings(List<DatabaseMapping> mappings) {
        if (CollectionUtils.isEmpty(mappings)) {
            return;
        }
        for (int i = 0; i < mappings.size(); i++) {
            DatabaseMapping mapping = mappings.get(i);
            if (mapping.getIndex() <= 0) {
                mapping.setIndex(i + 1);
            }
            List<TableMapping> tableMappings = mapping.getTableMappings();
            if (CollectionUtils.isEmpty(tableMappings)) {
                continue;
            }
            for (int j = 0; j < tableMappings.size(); j++) {
                TableMapping row = tableMappings.get(j);
                if (row.getIndex() <= 0) {
                    row.setIndex(j + 1);
                }
            }
            tableMappings.sort(Comparator.comparingInt(TableMapping::getIndex));
            for (int j = 0; j < tableMappings.size(); j++) {
                tableMappings.get(j).setIndex(j + 1);
            }
        }
        mappings.sort(Comparator.comparingInt(DatabaseMapping::getIndex));
        for (int i = 0; i < mappings.size(); i++) {
            mappings.get(i).setIndex(i + 1);
        }
    }

    private void fillTaskOnAdd(DatabaseSyncTask task, Map<String, String> params) {
        if (StringUtil.isBlank(task.getId())) {
            task.setId(String.valueOf(snowflakeIdWorker.nextId()));
            task.setStatus(CommonTaskStatusEnum.READY.getCode());
            task.setType(CommonTaskTypeEnum.DATABASE_SYNC.name());
        }
        long now = Instant.now().toEpochMilli();
        task.setCreateTime(task.getCreateTime() == null ? now : task.getCreateTime());
        task.setUpdateTime(now);
        task.setName(params.get("name"));
        fillSyncStrategy(task, params);
    }

    private void fillTaskOnEdit(DatabaseSyncTask task, Map<String, String> params) {
        String name = params.get("name");
        if (StringUtil.isBlank(name)) {
            throw new BizException("任务名称不能为空");
        }
        task.setName(name);
        task.setUpdateTime(Instant.now().toEpochMilli());
        fillSyncStrategy(task, params);
    }

    private void fillSyncStrategy(DatabaseSyncTask task, Map<String, String> params) {
        task.setEnableCopySchema(StringUtil.isNotBlank(params.get("enableCopySchema")));
        task.setEnableCopyData(StringUtil.isNotBlank(params.get("enableCopyData")));
        task.setOverwriteSchema(task.isEnableCopySchema() && StringUtil.isNotBlank(params.get("overwriteSchema")));
        task.setOverwriteData(task.isEnableCopyData() && StringUtil.isNotBlank(params.get("overwriteData")));
    }

    private void assertNotRunning(String taskId) {
        if (taskService.isRunning(taskId)) {
            throw new BizException("任务正在运行，请先停止");
        }
    }

    private void clearTableGroups(String taskId) {
        profileComponent.getTableGroupAll(taskId)
                .forEach(group -> profileComponent.removeTableGroup(group.getId()));
    }

    private void validateMappingConnectors(List<DatabaseMapping> mappings) {
        for (int i = 0; i < mappings.size(); i++) {
            DatabaseMapping mapping = mappings.get(i);
            if (StringUtil.isBlank(mapping.getSourceConnectorId())) {
                throw new BizException("库映射 " + (i + 1) + " 缺少源端连接器");
            }
            if (StringUtil.isBlank(mapping.getTargetConnectorId())) {
                throw new BizException("库映射 " + (i + 1) + " 缺少目标端连接器");
            }
        }
    }

    private DatabaseSyncTaskVO convertTask2Vo(DatabaseSyncTask task) {
        if (task == null) {
            return null;
        }
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(task.getId());
        List<DatabaseMapping> mappings = rebuildDatabaseMappingsFromTableGroups(tableGroups);
        DatabaseMapping first = CollectionUtils.isEmpty(mappings) ? null : mappings.get(0);
        Connector source = first == null ? null : profileComponent.getConnector(first.getSourceConnectorId());
        Connector target = first == null ? null : profileComponent.getConnector(first.getTargetConnectorId());
        DatabaseSyncTaskVO vo = new DatabaseSyncTaskVO(source, target);
        BeanUtils.copyProperties(task, vo);
        vo.setDatabaseMappings(mappings);
        int mappingCount = CollectionUtils.isEmpty(mappings) ? 0 : mappings.size();
        vo.setMappingCount(mappingCount);
        return vo;
    }

    /**
     * 库表关联只存 table_group；保存/展示时从 table_group 还原 DatabaseMapping 视图。
     */
    private List<DatabaseMapping> rebuildDatabaseMappingsFromTableGroups(List<TableGroup> tableGroups) {
        if (CollectionUtils.isEmpty(tableGroups)) {
            return Collections.emptyList();
        }
        Map<String, DatabaseMapping> mappingMap = new LinkedHashMap<>();
        List<TableGroup> sorted = tableGroups.stream()
                .sorted(Comparator.comparingInt(TableGroup::getIndex))
                .collect(Collectors.toList());
        for (TableGroup group : sorted) {
            String key = String.join("|",
                    StringUtil.getIfBlank(group.getSourceConnectorId(), ""),
                    StringUtil.getIfBlank(group.getTargetConnectorId(), ""),
                    StringUtil.getIfBlank(group.getSourceDatabase(), ""),
                    StringUtil.getIfBlank(group.getTargetDatabase(), ""),
                    StringUtil.getIfBlank(group.getSourceSchema(), ""),
                    StringUtil.getIfBlank(group.getTargetSchema(), ""));
            DatabaseMapping mapping = mappingMap.computeIfAbsent(key, k -> {
                DatabaseMapping dm = new DatabaseMapping();
                dm.setSourceConnectorId(group.getSourceConnectorId());
                dm.setTargetConnectorId(group.getTargetConnectorId());
                dm.setSourceDatabase(group.getSourceDatabase());
                dm.setTargetDatabase(group.getTargetDatabase());
                dm.setSourceSchema(group.getSourceSchema());
                dm.setTargetSchema(group.getTargetSchema());
                dm.setTableMappings(new ArrayList<>());
                return dm;
            });
            if (group.getSourceTable() == null || group.getTargetTable() == null) {
                continue;
            }
            TableMapping tm = new TableMapping();
            tm.setIndex(group.getIndex());
            tm.setSourceTable(group.getSourceTable().getName());
            tm.setTargetTable(group.getTargetTable().getName());
            mapping.getTableMappings().add(tm);
        }
        List<DatabaseMapping> result = new ArrayList<>(mappingMap.values());
        for (int i = 0; i < result.size(); i++) {
            result.get(i).setIndex(i + 1);
        }
        return result;
    }

    private void persistTableGroupsFromMappings(String taskId, List<DatabaseMapping> mappings) {
        if (StringUtil.isBlank(taskId) || CollectionUtils.isEmpty(mappings)) {
            return;
        }
        int sortIndex = 0;
        long now = Instant.now().toEpochMilli();
        for (DatabaseMapping mapping : mappings) {
            List<TableMapping> tableMappings = mapping.getSortedTableMappings();
            if (CollectionUtils.isEmpty(tableMappings)) {
                continue;
            }
            for (TableMapping tableMapping : tableMappings) {
                sortIndex++;
                TableGroup group = new TableGroup();
                group.setId(String.valueOf(snowflakeIdWorker.nextId()));
                group.setTaskId(taskId);
                group.setIndex(sortIndex);
                group.setSourceConnectorId(mapping.getSourceConnectorId());
                group.setTargetConnectorId(mapping.getTargetConnectorId());
                group.setSourceDatabase(StringUtil.getIfBlank(mapping.getSourceDatabase(), StringUtil.EMPTY));
                group.setTargetDatabase(StringUtil.getIfBlank(mapping.getTargetDatabase(), StringUtil.EMPTY));
                group.setSourceSchema(StringUtil.getIfBlank(mapping.getSourceSchema(), StringUtil.EMPTY));
                group.setTargetSchema(StringUtil.getIfBlank(mapping.getTargetSchema(), StringUtil.EMPTY));
                Table sourceTable = new Table();
                sourceTable.setName(tableMapping.getSourceTable());
                sourceTable.setType(TableTypeEnum.TABLE.getCode());
                group.setSourceTable(sourceTable);
                Table targetTable = new Table();
                targetTable.setName(tableMapping.getTargetTable());
                targetTable.setType(TableTypeEnum.TABLE.getCode());
                group.setTargetTable(targetTable);
                group.setCreateTime(now);
                group.setUpdateTime(now);
                profileComponent.addTableGroup(group);
            }
        }
    }
}