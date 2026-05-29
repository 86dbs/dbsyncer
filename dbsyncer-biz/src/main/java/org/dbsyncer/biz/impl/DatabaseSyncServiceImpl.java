/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.DatabaseSyncService;
import org.dbsyncer.biz.vo.DatabaseSyncTaskVO;
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
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.DatabaseMapping;
import org.dbsyncer.sdk.model.DatabaseMigrationProgressComputer;
import org.dbsyncer.sdk.model.DatabaseMigrationSyncTask;
import org.dbsyncer.sdk.model.Table;
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
    private TaskService<DatabaseMigrationSyncTask> taskService;

    @Resource
    private StorageService storageService;

    @Override
    public DatabaseSyncTaskVO get(String id) {
        DatabaseMigrationSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        return convertTask2Vo(task);
    }

    @Override
    public String add(Map<String, String> params) {
        String name = params.get("name");
        if (StringUtil.isBlank(name)) {
            throw new BizException("任务名称不能为空");
        }
        String sourceConnectorId = params.get("sourceConnectorId");
        String targetConnectorId = params.get("targetConnectorId");
        if (StringUtil.isBlank(sourceConnectorId)) {
            throw new BizException("源端连接器不能为空");
        }
        if (StringUtil.isBlank(targetConnectorId)) {
            throw new BizException("目标端连接器不能为空");
        }

        List<DatabaseMapping> mappings = parseDatabaseMappings(params.get("databaseMappingsJson"));
        if (CollectionUtils.isEmpty(mappings)) {
            throw new BizException("请至少添加一组库映射");
        }
        normalizeAndSortMappings(mappings);

        DatabaseMigrationSyncTask task = new DatabaseMigrationSyncTask();
        fillTaskOnAdd(task, params);
        task.setSourceConnectorId(sourceConnectorId);
        task.setTargetConnectorId(targetConnectorId);
        task.setDatabaseMappings(mappings);

        String taskId = taskService.add(task);
        logger.info("整库迁移任务已保存: id={}, name={}, mappingCount={}", taskId, name, mappings.size());
        return taskId;
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get("id");
        Assert.hasText(id, "任务 ID 不能为空");
        DatabaseMigrationSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        assertNotRunning(id);

        List<DatabaseMapping> mappings = parseDatabaseMappings(params.get("databaseMappingsJson"));
        if (CollectionUtils.isEmpty(mappings)) {
            throw new BizException("请至少添加一组库映射");
        }
        normalizeAndSortMappings(mappings);

        fillTaskOnEdit(task, params);
        task.setSourceConnectorId(params.get("sourceConnectorId"));
        task.setTargetConnectorId(params.get("targetConnectorId"));
        task.setDatabaseMappings(mappings);
        return taskService.edit(task);
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
        DatabaseMigrationSyncTask task = taskService.get(id);
        Assert.notNull(task, "任务不存在");
        if (CollectionUtils.isEmpty(task.getDatabaseMappings())) {
            throw new BizException("任务未配置库映射，无法启动");
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
            if (item instanceof DatabaseMigrationSyncTask) {
                DatabaseMigrationSyncTask task = (DatabaseMigrationSyncTask) item;
                DatabaseSyncTaskVO vo = convertTask2Vo(task);
                if (vo != null) {
                    List<TableGroup> tableGroups = profileComponent.getTableGroupAll(task.getId());
                    int tableCount = CollectionUtils.isEmpty(tableGroups) ? 0 : tableGroups.size();
                    vo.setProgress(DatabaseMigrationProgressComputer.calculateProgressPercent(task, tableCount));
                    vo.setErrorCount(countMigrationDetailErrors(task.getId()));
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
                .filter(DatabaseMigrationSyncTask.class::isInstance)
                .map(t -> convertTask2Vo((DatabaseMigrationSyncTask) t))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public Paging searchResult(Map<String, String> params) {
        String taskId = params.get("taskId");
        Assert.hasText(taskId, "taskId 不能为空");
        Query query = new Query(NumberUtil.toInt(params.get("pageNum"), 1), NumberUtil.toInt(params.get("pageSize"), 10));
        query.setType(StorageEnum.DATABASE_SYNC_DETAIL);
        query.addFilter(ConfigConstant.TASK_ID, taskId);
        query.setSelectFlied(getMigrationDetailSelect());
        query.addOrderBy(ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL, SortEnum.DESC);
        query.addOrderBy(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortEnum.DESC);
        return storageService.query(query);
    }

    @Override
    public Map<String, Object> previewTables(Map<String, String> params) {
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
                "database-syncer-preview",
                connectorId, database, schema,
                connectorId, database, schema,
                true);
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId());
        List<Table> tables = connectorFactory.getTables(connectorInstance, context);
        if (CollectionUtils.isEmpty(tables)) {
            return buildPreviewTablesResult(Collections.emptyList(), 0, offset, limit);
        }

        if (StringUtil.isNotBlank(searchKey)) {
            String key = searchKey.toUpperCase();
            tables = tables.stream()
                    .filter(t -> t.getName() != null && t.getName().toUpperCase().contains(key))
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

        Map<String, Object> result = buildPreviewTablesResult(pageRows, realTotal, offset, limit);
        result.put("typeCounts", typeCounts);
        return result;
    }

    private List<DatabaseMapping> parseDatabaseMappings(String mappingsJson) {
        if (StringUtil.isBlank(mappingsJson)) {
            return Collections.emptyList();
        }
        List<DatabaseMapping> mappings = JsonUtil.jsonToArray(mappingsJson, DatabaseMapping.class);
        return mappings == null ? Collections.emptyList() : mappings;
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
            List<DatabaseMapping.TableMapping> tableMappings = mapping.getTableMappings();
            if (CollectionUtils.isEmpty(tableMappings)) {
                continue;
            }
            for (int j = 0; j < tableMappings.size(); j++) {
                DatabaseMapping.TableMapping row = tableMappings.get(j);
                if (row.getIndex() <= 0) {
                    row.setIndex(j + 1);
                }
            }
            tableMappings.sort(Comparator.comparingInt(DatabaseMapping.TableMapping::getIndex));
            for (int j = 0; j < tableMappings.size(); j++) {
                tableMappings.get(j).setIndex(j + 1);
            }
        }
        mappings.sort(Comparator.comparingInt(DatabaseMapping::getIndex));
        for (int i = 0; i < mappings.size(); i++) {
            mappings.get(i).setIndex(i + 1);
        }
    }

    private void fillTaskOnAdd(DatabaseMigrationSyncTask task, Map<String, String> params) {
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

    private void fillTaskOnEdit(DatabaseMigrationSyncTask task, Map<String, String> params) {
        String name = params.get("name");
        if (StringUtil.isBlank(name)) {
            throw new BizException("任务名称不能为空");
        }
        String sourceConnectorId = params.get("sourceConnectorId");
        String targetConnectorId = params.get("targetConnectorId");
        if (StringUtil.isBlank(sourceConnectorId)) {
            throw new BizException("源端连接器不能为空");
        }
        if (StringUtil.isBlank(targetConnectorId)) {
            throw new BizException("目标端连接器不能为空");
        }
        task.setName(name);
        task.setUpdateTime(Instant.now().toEpochMilli());
        fillSyncStrategy(task, params);
    }

    private void fillSyncStrategy(DatabaseMigrationSyncTask task, Map<String, String> params) {
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

    private DatabaseSyncTaskVO convertTask2Vo(DatabaseMigrationSyncTask task) {
        if (task == null) {
            return null;
        }
        Connector source = profileComponent.getConnector(task.getSourceConnectorId());
        Connector target = profileComponent.getConnector(task.getTargetConnectorId());
        DatabaseSyncTaskVO vo = new DatabaseSyncTaskVO(source, target);
        BeanUtils.copyProperties(task, vo);
        int mappingCount = CollectionUtils.isEmpty(task.getDatabaseMappings()) ? 0 : task.getDatabaseMappings().size();
        vo.setMappingCount(mappingCount);
        return vo;
    }

    private Map<String, Object> buildPreviewTablesResult(List<Map<String, Object>> data, int total, int offset, int limit) {
        Map<String, Object> result = new HashMap<>(8);
        result.put("data", data);
        result.put("total", total);
        result.put("offset", offset);
        result.put("limit", limit);
        int from = Math.min(offset, total);
        int to = Math.min(from + limit, total);
        result.put("hasMore", to < total);
        return result;
    }

    private long countMigrationDetailErrors(String taskId) {
        Query query = new Query(1, 1);
        query.setType(StorageEnum.DATABASE_SYNC_DETAIL);
        query.addFilter(ConfigConstant.TASK_ID, taskId);
        query.addFilter(ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL, FilterEnum.GT, 0);
        query.setQueryTotal(true);
        Paging paging = storageService.query(query);
        return paging != null ? paging.getTotal() : 0;
    }

    private static Set<String> getMigrationDetailSelect() {
        Set<String> fields = new HashSet<>();
        fields.add(ConfigConstant.CONFIG_MODEL_ID);
        fields.add(ConfigConstant.TASK_ID);
        fields.add(ConfigConstant.CONFIG_MODEL_TYPE);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_TABLE_INDEX);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_DATABASE);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_SCHEMA);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_DATABASE);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_SOURCE_TABLE);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_TARGET_TABLE);
        fields.add(ConfigConstant.TASK_SOURCE_TOTAL);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_SUCCESS_TOTAL);
        fields.add(ConfigConstant.DATABASE_SYNC_DETAIL_FAIL_TOTAL);
        fields.add(ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        fields.add(ConfigConstant.CONFIG_MODEL_CREATE_TIME);
        return fields;
    }
}
