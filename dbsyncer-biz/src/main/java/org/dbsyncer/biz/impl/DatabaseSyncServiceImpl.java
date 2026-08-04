/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.DatabaseSyncService;
import org.dbsyncer.biz.vo.DatabaseMappingVO;
import org.dbsyncer.biz.vo.DatabaseSyncTaskVO;
import org.dbsyncer.biz.vo.TablePreviewVO;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.parser.util.DatabaseSyncMappingUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.CommonTaskSnapshot;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.TableMapping;
import org.dbsyncer.sdk.spi.DatabaseSyncDetailService;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.sdk.util.DatabaseSyncProgressUtil;
import org.dbsyncer.sdk.util.TaskSnapshotUtil;
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
    private TaskProfile taskProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private TaskService<DatabaseSyncTask> taskService;

    @Resource
    private DatabaseSyncDetailService databaseSyncDetailService;

    @Resource
    private ParserComponent parserComponent;

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
        List<DatabaseMappingVO> mappings = parseDatabaseMappings(params.get("databaseMappingsJson"));
        checkDatabaseMapping(mappings);
        if (CollectionUtils.isEmpty(mappings)) {
            throw new BizException("请至少添加一组库映射");
        }
        normalizeAndSortMappings(mappings);
        validateMappingConnectors(mappings);
        validateTableMappings(mappings);

        DatabaseSyncTask task = new DatabaseSyncTask();
        fillTaskOnAdd(task, params);
        task.setDatabaseMappings(toPersistMappings(mappings));
        // 任务需先落库（含任务级 Meta）才能按 taskId 建连与写 table_group；失败则整单回滚
        String taskId = taskService.add(task);
        try {
            tableGroupProfile.addTableGroupBatch(buildTableGroups(taskId, mappings));
        } catch (Exception e) {
            try {
                taskService.delete(taskId);
            } catch (Exception cleanupEx) {
                logger.error("整库迁移任务创建失败后回滚删除失败: id={}", taskId, cleanupEx);
            }
            throw new BizException(e.getMessage(), e);
        }
        // 预建明细分表，详情页 JOIN 查询不依赖任务是否已写出数据
        taskProfile.createRunDetailTable(taskId);
        logger.info("整库迁移任务已保存: id={}, name={}, mappingCount={}", taskId, name, mappings.size());
        return taskId;
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get("id");
        Assert.hasText(id, "任务 ID 不能为空");
        DatabaseSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        if (taskService.isRunning(id)) {
            throw new BizException("任务正在运行，请先停止");
        }
        List<DatabaseMappingVO> mappings = parseDatabaseMappings(params.get("databaseMappingsJson"));
        checkDatabaseMapping(mappings);
        if (CollectionUtils.isEmpty(mappings)) {
            throw new BizException("请至少添加一组库映射");
        }
        normalizeAndSortMappings(mappings);
        validateMappingConnectors(mappings);
        validateTableMappings(mappings);
        fillTaskOnEdit(task, params);
        task.setDatabaseMappings(toPersistMappings(mappings));
        // 先物化新映射（连库读元数据），失败则不碰旧 table_group
        List<TableGroup> newGroups = buildTableGroups(id, mappings);
        List<TableGroup> oldGroups = new ArrayList<>();
        tableGroupProfile.pageScanTableGroups(id, ConfigConstant.PAGE_SIZE, oldGroups::addAll);
        try {
            tableGroupProfile.removeTableGroupsByTaskId(id);
            tableGroupProfile.addTableGroupBatch(newGroups);
        } catch (Exception e) {
            restoreTableGroups(id, oldGroups);
            throw new BizException(e.getMessage(), e);
        }
        // 映射落库成功后再清运行结果与任务级 Meta，避免写失败留下半残任务
        taskProfile.clearRunData(id);
        taskProfile.resetRunProgress(id);
        return taskService.edit(task);
    }

    private void checkDatabaseMapping(List<DatabaseMappingVO> mappings) {
        for (DatabaseMappingVO mapping : mappings) {
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
        if (taskService.isRunning(id)) {
            throw new BizException("任务正在运行，请先停止");
        }
        taskService.delete(id);
        return "删除成功";
    }

    @Override
    public String start(String id) {
        Assert.hasText(id, "任务 ID 不能为空");
        DatabaseSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        if (CollectionUtils.isEmpty(task.getDatabaseMappings())) {
            throw new BizException("任务未配置库映射，无法启动");
        }
        if (tableGroupProfile.getTableGroupCount(id) <= 0) {
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
                    int tableCount = tableGroupProfile.getTableGroupCount(task.getId());
                    Meta taskMeta = metaProfile.getMetaByTaskId(task.getId(), TaskLevelEnum.TASK);
                    boolean roundDone = taskMeta != null && DatabaseSyncProgressUtil.isRoundDone(taskMeta.getState());
                    Map<Integer, Integer> mappingStatus = DatabaseSyncProgressUtil.readMappingStatus(
                            taskMeta == null ? null : taskMeta.getSnapshot());
                    List<CommonTaskSnapshot> tableSnapshots = collectTableSnapshots(task.getId());
                    vo.setProgress(DatabaseSyncProgressUtil.calculateProgressPercent(
                            task, tableCount, vo.getMappingCount(), roundDone, mappingStatus, tableSnapshots));
                    vo.setTotalTableCount(tableCount);
                    vo.setCompletedTableCount(DatabaseSyncProgressUtil.countCompletedTables(
                            task, tableCount, roundDone, mappingStatus, tableSnapshots));
                    vo.setErrorCount(0L);
                    if (taskMeta != null) {
                        if (taskMeta.getFail() != null) {
                            vo.setErrorCount(taskMeta.getFail().get());
                        }
                        vo.setMetaState(taskMeta.getState());
                        vo.setBeginTime(taskMeta.getBeginTime() > 0 ? taskMeta.getBeginTime() : null);
                        vo.setEndTime(taskMeta.getEndTime() > 0 ? taskMeta.getEndTime() : null);
                    }
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
        // 按所选库/schema 建临时连接查表（PostgreSQL 等无法靠 setCatalog 切库，须重建 JDBC URL）
        String previewInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(
                "database-sync-preview", connectorId, database, schema, ConnectorInstanceUtil.SOURCE_SUFFIX);
        try {
            connectorFactory.connect(previewInstanceId, connector.getConfig(), database, schema);
            ConnectorInstance connectorInstance = connectorFactory.connect(previewInstanceId);
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
            List<Table> pageRows = tables.stream().skip(offset).limit(limit).collect(Collectors.toList());
            TablePreviewVO result = TablePreviewVO.of(pageRows, realTotal, String.valueOf(offset), limit);
            result.setTypeCounts(typeCounts);
            return result;
        } finally {
            connectorFactory.disconnect(previewInstanceId);
        }
    }

    private List<DatabaseMappingVO> parseDatabaseMappings(String mappingsJson) {
        if (StringUtil.isBlank(mappingsJson)) {
            return Collections.emptyList();
        }
        List<DatabaseMappingVO> mappings = JsonUtil.jsonToArray(mappingsJson, DatabaseMappingVO.class);
        return mappings == null ? Collections.emptyList() : mappings;
    }

    /**
     * 入参 VO 转 task.JSON 持久化库映射（仅库维）。
     */
    private List<DatabaseMapping> toPersistMappings(List<DatabaseMappingVO> mappings) {
        if (CollectionUtils.isEmpty(mappings)) {
            return new ArrayList<>();
        }
        List<DatabaseMapping> result = new ArrayList<>(mappings.size());
        for (DatabaseMappingVO mapping : mappings) {
            if (mapping != null) {
                result.add(mapping.toDatabaseMapping());
            }
        }
        return result;
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
    private void normalizeAndSortMappings(List<DatabaseMappingVO> mappings) {
        if (CollectionUtils.isEmpty(mappings)) {
            return;
        }
        for (int i = 0; i < mappings.size(); i++) {
            DatabaseMappingVO mapping = mappings.get(i);
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
        mappings.sort(Comparator.comparingInt(DatabaseMappingVO::getIndex));
        for (int i = 0; i < mappings.size(); i++) {
            mappings.get(i).setIndex(i + 1);
        }
    }

    private void fillTaskOnAdd(DatabaseSyncTask task, Map<String, String> params) {
        if (StringUtil.isBlank(task.getId())) {
            task.setId(String.valueOf(snowflakeIdWorker.nextId()));
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

    /**
     * 编辑失败时恢复旧 table_group，避免任务无表映射。
     * 运行历史分表在映射写成功后才清理，此处仍可保留旧明细。
     */
    private void restoreTableGroups(String taskId, List<TableGroup> oldGroups) {
        try {
            tableGroupProfile.removeTableGroupsByTaskId(taskId);
        } catch (Exception cleanupEx) {
            logger.error("编辑失败回滚：清理残留 table_group 失败: taskId={}", taskId, cleanupEx);
        }
        if (CollectionUtils.isEmpty(oldGroups)) {
            return;
        }
        try {
            tableGroupProfile.addTableGroupBatch(oldGroups);
        } catch (Exception restoreEx) {
            logger.error("编辑失败回滚：恢复旧 table_group 失败: taskId={}", taskId, restoreEx);
        }
    }

    private void validateMappingConnectors(List<? extends DatabaseMapping> mappings) {
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

    /**
     * 表映射目标表名不能为空（add/edit 对称校验）
     */
    private void validateTableMappings(List<DatabaseMappingVO> mappings) {
        if (CollectionUtils.isEmpty(mappings)) {
            return;
        }
        for (int i = 0; i < mappings.size(); i++) {
            List<TableMapping> tableMappings = mappings.get(i).getTableMappings();
            if (CollectionUtils.isEmpty(tableMappings)) {
                continue;
            }
            for (TableMapping row : tableMappings) {
                if (row == null) {
                    continue;
                }
                if (StringUtil.isBlank(row.getTargetTable())) {
                    String source = StringUtil.isNotBlank(row.getSourceTable()) ? row.getSourceTable() : "";
                    throw new BizException(String.format("库映射 %d：源表「%s」的目标表名不能为空", i + 1, source));
                }
            }
        }
    }

    private DatabaseSyncTaskVO convertTask2Vo(DatabaseSyncTask task) {
        if (task == null) {
            return null;
        }
        List<TableGroup> tableGroups = new ArrayList<>();
        tableGroupProfile.pageScanTableGroups(task.getId(), ConfigConstant.PAGE_SIZE, tableGroups::addAll);
        List<DatabaseMappingVO> mappingViews = buildDatabaseMappingVo(
                DatabaseSyncMappingUtil.sortByIndex(task.getDatabaseMappings()), tableGroups);
        DatabaseMappingVO first = CollectionUtils.isEmpty(mappingViews) ? null : mappingViews.get(0);
        Connector source = first == null ? null : profileComponent.getConnector(first.getSourceConnectorId());
        Connector target = first == null ? null : profileComponent.getConnector(first.getTargetConnectorId());
        DatabaseSyncTaskVO vo = new DatabaseSyncTaskVO(source, target);
        BeanUtils.copyProperties(task, vo);
        // 覆盖 BeanUtils 写入的仅库维映射，挂上 table_group 表映射供编辑页
        vo.setMappingViews(mappingViews);
        vo.setMappingCount(CollectionUtils.isEmpty(mappingViews) ? 0 : mappingViews.size());
        return vo;
    }

    private List<CommonTaskSnapshot> collectTableSnapshots(String taskId) {
        List<String> ids = tableGroupProfile.listTableGroupIds(taskId);
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }
        Map<String, Meta> metaMap = metaProfile.getDetailMetaMap(ids);
        List<CommonTaskSnapshot> snapshots = new ArrayList<>(ids.size());
        for (String groupId : ids) {
            if (StringUtil.isBlank(groupId)) {
                snapshots.add(null);
                continue;
            }
            Meta meta = metaMap == null ? null : metaMap.get(groupId);
            snapshots.add(meta == null ? null : TaskSnapshotUtil.readTableSnapshot(meta.getSnapshot()));
        }
        return snapshots;
    }

    /**
     * 将 table_group 按库键挂到库映射 VO 的 tableMappings（编辑页回显）。
     */
    private List<DatabaseMappingVO> buildDatabaseMappingVo(List<DatabaseMapping> mappings, List<TableGroup> tableGroups) {
        if (CollectionUtils.isEmpty(mappings)) {
            return new ArrayList<>();
        }
        Map<String, List<TableGroup>> groupsByKey = new HashMap<>();
        if (!CollectionUtils.isEmpty(tableGroups)) {
            for (TableGroup group : tableGroups) {
                if (group == null) {
                    continue;
                }
                groupsByKey.computeIfAbsent(group.buildDatabaseMappingKey(), k -> new ArrayList<>()).add(group);
            }
        }
        List<DatabaseMappingVO> result = new ArrayList<>(mappings.size());
        for (DatabaseMapping src : mappings) {
            if (src == null) {
                continue;
            }
            DatabaseMappingVO vo = DatabaseMappingVO.from(src);
            List<TableMapping> tableMappings = new ArrayList<>();
            List<TableGroup> groups = groupsByKey.getOrDefault(src.buildDatabaseMappingKey(), Collections.emptyList());
            groups.stream().sorted(Comparator.comparingInt(TableGroup::getIndex)).forEach(group -> {
                Table sourceTable = group.getSourceTable();
                Table targetTable = group.getTargetTable();
                if (sourceTable != null && targetTable != null) {
                    tableMappings.add(DatabaseSyncMappingUtil.toTableMapping(
                            sourceTable.getName(), targetTable.getName(), group.getIndex()));
                }
            });
            vo.setTableMappings(tableMappings);
            result.add(vo);
        }
        return result;
    }

    /**
     * 按库映射物化 table_group（连源端读表元数据），不落库。
     */
    private List<TableGroup> buildTableGroups(String taskId, List<DatabaseMappingVO> mappings) {
        if (StringUtil.isBlank(taskId) || CollectionUtils.isEmpty(mappings)) {
            return Collections.emptyList();
        }
        List<TableGroup> groups = new ArrayList<>();
        int sortIndex = 0;
        long now = Instant.now().toEpochMilli();
        for (DatabaseMappingVO mapping : mappings) {
            List<TableMapping> tableMappings = mapping.getSortedTableMappings();
            if (CollectionUtils.isEmpty(tableMappings)) {
                continue;
            }
            Connector sourceConnector = profileComponent.getConnector(mapping.getSourceConnectorId());
            Assert.notNull(sourceConnector, "源连接器不存在");
            Assert.notNull(sourceConnector.getConfig(), "源连接器配置不存在");
            // 与 ParserComponentImpl#getMetaInfo 的实例ID保持一致（mappingId + connectorId + suffix）
            connectorFactory.connect(
                    ConnectorInstanceUtil.buildConnectorInstanceId(taskId, mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX),
                    sourceConnector.getConfig(),
                    mapping.getSourceDatabase(),
                    mapping.getSourceSchema()
            );

            List<String> sourceNames = tableMappings.stream().map(TableMapping::getSourceTable).collect(Collectors.toList());
            Map<String, Table> sourceMetaMap = loadMetaTableMap(taskId, mapping, sourceNames);

            for (TableMapping tableMapping : tableMappings) {
                Table sourceTable = sourceMetaMap.get(tableMapping.getSourceTable());
                if (sourceTable == null) {
                    throw new BizException(String.format("源表 [%s] 不存在或无法读取元数据", tableMapping.getSourceTable()));
                }
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
                group.setSourceTable(sourceTable);
                Table targetTable = new Table();
                targetTable.setName(tableMapping.getTargetTable());
                targetTable.setType(TableTypeEnum.TABLE.getCode());
                group.setTargetTable(targetTable);
                group.setCreateTime(now);
                group.setUpdateTime(now);
                groups.add(group);
            }
        }
        return groups;
    }

    public Map<String, Table> loadMetaTableMap(String taskId, DatabaseMappingVO ctx, List<String> tableNames) {
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(
                taskId,
                ctx.getSourceConnectorId(),
                ctx.getSourceDatabase(),
                ctx.getSourceSchema(),
                ctx.getTargetConnectorId(),
                ctx.getTargetDatabase(),
                ctx.getTargetSchema(),
                true);
        tableNames.stream().distinct().forEach(context::addTablePattern);
        List<MetaInfo> metaInfos = parserComponent.getMetaInfo(context);
        Map<String, Table> tableMap = new HashMap<>(metaInfos.size());
        for (MetaInfo metaInfo : metaInfos) {
            if (metaInfo == null || StringUtil.isBlank(metaInfo.getTable())) {
                continue;
            }
            Table table = new Table();
            table.setName(metaInfo.getTable());
            table.setType(metaInfo.getTableType());
            table.setColumn(metaInfo.getColumn());
            tableMap.put(metaInfo.getTable(), table);
        }
        return tableMap;
    }

}