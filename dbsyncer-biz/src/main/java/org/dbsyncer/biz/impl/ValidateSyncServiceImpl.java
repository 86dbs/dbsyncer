/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.biz.checker.impl.mapping.MappingChecker;
import org.dbsyncer.biz.checker.impl.tablegroup.ValidateSyncTableGroupChecker;
import org.dbsyncer.biz.task.ValidateSyncMatchTableTask;
import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.common.dispatch.DispatchTaskService;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.CommonTaskTriggerEnum;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskDetailProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.enums.TaskDetailMetricEnum;
import org.dbsyncer.parser.enums.TaskDetailStatusEnum;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.TaskDetailQuery;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.model.CommonTaskSnapshot;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.sdk.spi.ValidateSyncDetailService;
import org.dbsyncer.sdk.util.TaskSnapshotUtil;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class ValidateSyncServiceImpl implements ValidateSyncService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private TaskService<ValidateSyncTask> taskService;

    @Resource
    private ValidateSyncDetailService validateSyncDetailService;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private TaskDetailProfile taskDetailProfile;

    @Resource
    private TableGroupService tableGroupService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private LogService logService;

    @Resource
    private PreloadTemplate preloadTemplate;

    @Resource
    private MappingChecker mappingChecker;

    @Resource
    private ValidateSyncTableGroupChecker validateSyncTableGroupChecker;

    @Resource
    private DispatchTaskService dispatchTaskService;

    /**
     * 任务启停锁
     */
    private final static Object LOCK = new Object();

    @Override
    public ValidateSyncTaskVO get(String id) {
        return convertTask2Vo(resolveTask(id));
    }

    /**
     * 解析任务：先走 TaskService 缓存，未命中再读存储（Follower 经写代理创建后本地缓存可能为空）。
     *
     * @param id 任务 ID
     * @return 任务；不存在为 null
     */
    private ValidateSyncTask resolveTask(String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        ValidateSyncTask task = taskService.get(id);
        if (task != null) {
            return task;
        }
        task = taskProfile.getTask(id, ValidateSyncTask.class);
        if (task != null) {
            logger.info("订正校验任务缓存未命中，已从存储加载: {}", id);
        }
        return task;
    }

    @Override
    public String add(Map<String, String> params) {
        ValidateSyncTask task = new ValidateSyncTask();
        checkTask(task, params);
        // 默认检查行数据
        task.setEnableRowData(true);
        // 关联同步任务
        String mappingId = params.get("mappingId");
        if (StringUtil.isNotBlank(mappingId)) {
            Mapping mapping = profileComponent.getMapping(mappingId);
            Assert.notNull(mapping, "mapping is not exist");
            task.setSourceConnectorId(mapping.getSourceConnectorId());
            task.setSourceDatabase(mapping.getSourceDatabase());
            task.setSourceSchema(mapping.getSourceSchema());
            task.setSourceTable(deepCopy(mapping.getSourceTable()));
            task.setTargetConnectorId(mapping.getTargetConnectorId());
            task.setTargetDatabase(mapping.getTargetDatabase());
            task.setTargetSchema(mapping.getTargetSchema());
            task.setTargetTable(deepCopy(mapping.getTargetTable()));
            task.setReadNum(mapping.getReadNum());
            task.setBatchNum(mapping.getBatchNum());
            task.setThreadNum(mapping.getThreadNum());
            // 复制表组列表
            tableGroupProfile.pageScanTableGroups(mappingId, ConfigConstant.PAGE_SIZE, tableGroupAll -> {
                if (CollectionUtils.isEmpty(tableGroupAll)) {
                    return;
                }
                for (TableGroup tableGroup : tableGroupAll) {
                    if (tableGroup == null) {
                        continue;
                    }
                    TableGroup newTable = deepCopy(tableGroup);
                    newTable.setId(String.valueOf(snowflakeIdWorker.nextId()));
                    newTable.setTaskId(task.getId());
                    tableGroupProfile.addTableGroup(newTable);
                }
            });
            // 合并任务公共字段
            mergeTaskColumn(task);
            String id = taskService.add(task);
            taskProfile.createRunDetailTable(id);
            validateSyncDetailService.syncTaskTableMetaDetails(id);
            preloadTemplate.reConnect(task);
            return id;
        } else {
            Assert.hasText(params.get("sourceConnectorId"), "数据源不能为空");
            Assert.hasText(params.get("targetConnectorId"), "目标源不能为空");
            task.setSourceConnectorId(params.get("sourceConnectorId"));
            task.setSourceDatabase(params.get("sourceDatabase"));
            task.setSourceSchema(params.get("sourceSchema"));
            task.setTargetConnectorId(params.get("targetConnectorId"));
            task.setTargetDatabase(params.get("targetDatabase"));
            task.setTargetSchema(params.get("targetSchema"));
            // 先持久化再建连，才能拉取到所有表
            String id = taskService.add(task);
            taskProfile.createRunDetailTable(id);
            preloadTemplate.reConnect(task);
            ValidateSyncTask validateSyncTask = refreshTablesAndGet(id);
            // 勾选「匹配相似表」时异步自动匹配，否则解析自定义表映射文本
            if (StringUtil.isNotBlank(params.get("autoMatchTable"))) {
                List<Table> sourceTables = validateSyncTask.getSourceTable();
                List<Table> targetTables = validateSyncTask.getTargetTable();
                if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
                    throw new BizException("未获取到源库或目标库表列表，无法匹配相似表");
                }
                submitValidateSyncMatchTableTask(id);
            } else {
                String tableGroups = params.get("tableGroups");
                if (StringUtil.isNotBlank(tableGroups)) {
                    matchCustomizedTableGroups(validateSyncTask, tableGroups);
                }
                validateSyncDetailService.syncTaskTableMetaDetails(id);
            }
            return id;
        }

    }

    private List<Table> deepCopy(List<Table> targetTable) {
        return JsonUtil.jsonToArray(JsonUtil.objToJson(targetTable), Table.class);
    }

    private TableGroup deepCopy(TableGroup tableGroup) {
        return JsonUtil.jsonToObj(JsonUtil.objToJson(tableGroup), TableGroup.class);
    }

    /**
     * 提交异步匹配相似表任务
     */
    private void submitValidateSyncMatchTableTask(String taskId) {
        ValidateSyncMatchTableTask task = new ValidateSyncMatchTableTask();
        task.setTaskId(taskId);
        task.setTaskService(taskService);
        task.setValidateSyncService(this);
        dispatchTaskService.execute(task);
    }

    /**
     * 自定义配置表映射关系（与 MappingServiceImpl 文本格式一致）
     */
    private void matchCustomizedTableGroups(ValidateSyncTask validateSyncTask, String tableGroups) {
        List<Table> sourceTables = validateSyncTask.getSourceTable();
        List<Table> targetTables = validateSyncTask.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            throw new BizException("未获取到源库或目标库表列表，无法解析表映射关系");
        }
        Map<String, Table> sourceTableMap = toTableNameMap(sourceTables);
        Map<String, Table> targetTableMap = toTableNameMap(targetTables);
        String[] lines = StringUtil.split(tableGroups, StringUtil.BREAK_LINE);
        // 数据源表|目标源表=源表字段A1*|目标字段A2*
        for (String line : lines) {
            if (StringUtil.isBlank(line)) {
                continue;
            }
            String[] tableGroup = StringUtil.split(line, StringUtil.EQUAL);
            String[] tableGroupNames = StringUtil.split(tableGroup[0], StringUtil.VERTICAL_LINE);
            if (tableGroupNames.length != 2) {
                continue;
            }
            Table sourceTable = sourceTableMap.get(tableGroupNames[0].toUpperCase(Locale.ROOT));
            Table targetTable = targetTableMap.get(tableGroupNames[1].toUpperCase(Locale.ROOT));
            if (sourceTable == null || targetTable == null) {
                logger.warn("自定义表映射未找到表: {} >> {}", tableGroupNames[0], tableGroupNames[1]);
                continue;
            }
            addMatchedTableGroup(validateSyncTask.getId(), sourceTable, targetTable,
                    tableGroup.length == 2 ? tableGroup[1] : StringUtil.EMPTY);
        }
    }

    private Map<String, Table> toTableNameMap(List<Table> tables) {
        Map<String, Table> map = new LinkedHashMap<>();
        for (Table table : tables) {
            if (table == null || StringUtil.isBlank(table.getName())) {
                continue;
            }
            map.putIfAbsent(table.getName().toUpperCase(Locale.ROOT), table);
        }
        return map;
    }

    /**
     * 单表匹配写入；失败只记日志，不中断其余表（对齐 MappingServiceImpl）
     */
    private boolean addMatchedTableGroup(String taskId, Table sourceTable, Table targetTable, String fieldMappings) {
        try {
            Map<String, String> params = new HashMap<>();
            params.put("taskId", taskId);
            params.put("sourceTable", sourceTable.getName());
            params.put("targetTable", targetTable.getName());
            params.put("sourceType", StringUtil.isNotBlank(sourceTable.getType()) ? sourceTable.getType() : TableTypeEnum.TABLE.getCode());
            params.put("targetType", StringUtil.isNotBlank(targetTable.getType()) ? targetTable.getType() : TableTypeEnum.TABLE.getCode());
            if (StringUtil.isNotBlank(fieldMappings)) {
                params.put("fieldMappings", fieldMappings);
            }
            addTableGroup(params);
            return true;
        } catch (Exception e) {
            logger.error("添加表映射失败: {} >> {}, {}", sourceTable.getName(), targetTable.getName(), e.getMessage());
            return false;
        }
    }

    @Override
    public String edit(Map<String, String> params) {
        ValidateSyncTask task = resolveTask(params.get("id"));
        if (task == null) {
            throw new BizException("任务不存在");
        }
        assertRunning(task.getId());
        checkTask(task, params);
        taskProfile.clearRunData(task.getId());
        taskProfile.resetRunProgress(task.getId());
        String sortedIds = params.get("sortedTableGroupIds");
        if (StringUtil.isNotBlank(sortedIds)) {
            List<TableGroup> groupAll = new ArrayList<>();
            tableGroupProfile.pageScanTableGroups(task.getId(), ConfigConstant.PAGE_SIZE, groupAll::addAll);
            if (!CollectionUtils.isEmpty(groupAll)) {
                mappingChecker.sortTableGroup(groupAll, params);
                for (TableGroup g : groupAll) {
                    validateSyncTableGroupChecker.mergeConfig(task, g);
                    profileComponent.editConfigModel(g);
                }
            }
        } else {
            tableGroupProfile.pageScanTableGroups(task.getId(), ConfigConstant.PAGE_SIZE, page -> {
                if (CollectionUtils.isEmpty(page)) {
                    return;
                }
                for (TableGroup g : page) {
                    if (g == null) {
                        continue;
                    }
                    validateSyncTableGroupChecker.mergeConfig(task, g);
                    profileComponent.editConfigModel(g);
                }
            });
        }
        String id = taskService.edit(task);
        // 编辑会清空运行明细，按当前表映射与开启类型重新对齐明细
        validateSyncDetailService.syncTaskTableMetaDetails(id);
        return id;
    }

    @Override
    public String copy(String id) {
        ValidateSyncTask task = resolveTask(id);
        Assert.notNull(task, "Task not found");
        String json = JsonUtil.objToJson(task);
        ValidateSyncTask newTask = JsonUtil.jsonToObj(json, ValidateSyncTask.class);
        newTask.setId(String.valueOf(snowflakeIdWorker.nextId()));
        newTask.setName(newTask.getName() + "(复制)");
        newTask.setType(CommonTaskTypeEnum.VALIDATE_SYNC.name());
        newTask.setUpdateTime(System.currentTimeMillis());
        String newId = taskService.add(newTask);
        // 深拷贝 table_group（关联已下沉到该表）
        tableGroupProfile.pageScanTableGroups(id, ConfigConstant.PAGE_SIZE, sourceGroups -> {
            if (CollectionUtils.isEmpty(sourceGroups)) {
                return;
            }
            long now = System.currentTimeMillis();
            for (TableGroup source : sourceGroups) {
                if (source == null) {
                    continue;
                }
                TableGroup copy = JsonUtil.jsonToObj(JsonUtil.objToJson(source), TableGroup.class);
                if (copy == null) {
                    continue;
                }
                copy.setId(String.valueOf(snowflakeIdWorker.nextId()));
                copy.setTaskId(newId);
                copy.setCreateTime(now);
                copy.setUpdateTime(now);
                tableGroupProfile.addTableGroup(copy);
            }
        });
        preloadTemplate.reConnect(newTask);
        taskProfile.createRunDetailTable(newId);
        validateSyncDetailService.syncTaskTableMetaDetails(newId);
        return newId;
    }

    @Override
    public String delete(String id) {
        assertRunning(id);
        taskService.delete(id);
        return "删除成功";
    }

    @Override
    public String start(String id) {
        Assert.isTrue(tableGroupProfile.getTableGroupCount(id) > 0, "任务未配置表映射，无法启动");
        Assert.isTrue(!dispatchTaskService.isRunning(id), "表映射正在匹配中，请稍候再启动");
        taskService.start(id);
        return "启动成功";
    }

    @Override
    public String stop(String id) {
        taskService.stop(id);
        return "停止成功";
    }

    @Override
    public Paging<ValidateSyncTaskVO> search(Map<String, String> params) {
        Paging search = taskService.search(params, CommonTaskTypeEnum.VALIDATE_SYNC);
        Collection data = search.getData();
        if (CollectionUtils.isEmpty(data)) {
            return search;
        }
        List<ValidateSyncTaskVO> list = new ArrayList<>();
        data.forEach(task -> {
            if (task instanceof ValidateSyncTask) {
                ValidateSyncTask t = (ValidateSyncTask) task;
                ValidateSyncTaskVO vo = convertTask2Vo(t);
                if (vo != null) {
                    long errorCount = 0L;
                    Meta taskMeta = metaProfile.getMetaByTaskId(t.getId(), TaskLevelEnum.TASK);
                    if (taskMeta != null && taskMeta.getDiff() != null) {
                        errorCount = taskMeta.getDiff().get();
                    }
                    vo.setErrorCount(errorCount);
                    int tableCount = tableGroupProfile.getTableGroupCount(t.getId());
                    vo.setTotalTableCount(tableCount);
                    boolean roundDone = taskMeta != null && CommonTaskStatusEnum.isDone(taskMeta.getState());
                    List<CommonTaskSnapshot> tableSnapshots = collectValidateTableSnapshots(t.getId());
                    vo.setCompletedTableCount(countCompletedTables(roundDone, tableCount, tableSnapshots));
                    vo.setProgress(calculateProgressPercent(roundDone, tableCount, tableSnapshots));
                    if (taskMeta != null) {
                        vo.setMetaState(taskMeta.getState());
                        vo.setBeginTime(taskMeta.getBeginTime() > 0 ? taskMeta.getBeginTime() : null);
                        vo.setEndTime(taskMeta.getEndTime() > 0 ? taskMeta.getEndTime() : null);
                    }
                    list.add(vo);
                }
            }
        });
        search.setData(list);
        return search;
    }

    @Override
    public Paging<TableGroup> searchTableGroup(Map<String, String> params) {
        String id = params.get("id");
        ValidateSyncTask task = resolveTask(id);
        if (task == null) {
            return null;
        }
        // 复用查表组
        params.put("mappingId", task.getId());
        return tableGroupService.search(params);
    }

    @Override
    public Paging<Table> searchTables(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String searchKey = params.get("searchKey");
        String tableNames = params.get("tableNames");

        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = Math.max(10, Math.min(200, NumberUtil.toInt(params.get("pageSize"), 50)));

        // 是否过滤已配置的表（exclude=1 表示不过滤）
        boolean excludeMapped = NumberUtil.toInt(params.get("exclude"), 0) != 1;

        ValidateSyncTask task = resolveTask(id);
        Assert.notNull(task, "task not found.");

        boolean isSource = !"target".equals(type);
        List<Table> tables = isSource ? task.getSourceTable() : task.getTargetTable();
        tables = CollectionUtils.isEmpty(tables) ? Collections.emptyList() : tables;

        // 已映射/已配置的表名
        Set<String> mappedTableNames;
        if (excludeMapped) {
            mappedTableNames = new HashSet<>();
            tableGroupProfile.pageScanTableGroups(id, ConfigConstant.PAGE_SIZE, page -> {
                if (CollectionUtils.isEmpty(page)) {
                    return;
                }
                for (TableGroup g : page) {
                    if (g == null) {
                        continue;
                    }
                    Table t = isSource ? g.getSourceTable() : g.getTargetTable();
                    if (t != null && StringUtil.isNotBlank(t.getName())) {
                        mappedTableNames.add(t.getName().toUpperCase(Locale.ROOT));
                    }
                }
            });
        } else {
            mappedTableNames = Collections.emptySet();
        }

        // 精准匹配（tableNames: a|b|c）
        Set<String> exactNames = new HashSet<>();
        if (StringUtil.isNotBlank(tableNames)) {
            String[] nameArray = StringUtil.split(tableNames, "\\|");
            if (nameArray != null) {
                Arrays.stream(nameArray)
                        .filter(StringUtil::isNotBlank)
                        .map(n -> n.toUpperCase(Locale.ROOT))
                        .forEach(exactNames::add);
            }
        }

        String key = StringUtil.trimToEmpty(searchKey).toUpperCase(Locale.ROOT);

        List<Table> filtered = tables.stream()
                .filter(Objects::nonNull)
                .filter(t -> StringUtil.isNotBlank(t.getName()))
                .filter(t -> mappedTableNames.isEmpty() || !mappedTableNames.contains(t.getName().toUpperCase(Locale.ROOT)))
                .filter(t -> exactNames.isEmpty() || exactNames.contains(t.getName().toUpperCase(Locale.ROOT)))
                .filter(t -> key.isEmpty() || t.getName().toUpperCase(Locale.ROOT).contains(key))
                .sorted(Comparator.comparing(Table::getName))
                .collect(Collectors.toList());

        Paging<Table> paging = new Paging<>(pageNum, pageSize);
        paging.setTotal(filtered.size());
        int offset = (pageNum - 1) * pageSize;
        if (offset >= 0 && offset < filtered.size()) {
            paging.setData(filtered.stream()
                    .skip(offset)
                    .limit(pageSize)
                    .collect(Collectors.toList()));
        }
        return paging;
    }

    @Override
    public Object result(String id) {
        return resolveTask(id);
    }

    @Override
    public List<ValidateSyncTaskVO> getAll() {
        return taskService.getTaskAll(CommonTaskTypeEnum.VALIDATE_SYNC).stream()
                .map(this::convertTask2Vo)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public Paging searchResult(Map<String, String> params) {
        String taskId = params.get("taskId");
        Assert.hasText(taskId, "任务ID不能为空");
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        String detailStatus = StringUtil.trimToEmpty(params.get("detailStatus"));
        return taskDetailProfile.queryResults(TaskDetailQuery.of(taskId)
                .setPage(pageNum, pageSize)
                .setDetailStatus(TaskDetailStatusEnum.from(detailStatus))
                .setStatusMetric(TaskDetailMetricEnum.DIFF));
    }

    @Override
    public Object getValidateResultDetail(String taskId, String id) {
        Assert.hasText(taskId, "任务ID不能为空");
        Assert.hasText(id, "明细ID不能为空");
        return taskDetailProfile.getDetail(TaskDetailQuery.of(taskId).setDetailId(id));
    }

    @Override
    public Object manualReviseDetail(String taskId, String detailId) {
        Assert.hasText(taskId, "任务ID不能为空");
        Assert.hasText(detailId, "明细ID不能为空");
        return validateSyncDetailService.manualRevise(taskId, detailId);
    }

    @Override
    public String refreshTables(String id) {
        refreshTablesAndGet(id);
        return id;
    }

    /**
     * 拉取并回写源/目标表列表，返回已刷新的任务对象（供创建后立即匹配使用）。
     */
    private ValidateSyncTask refreshTablesAndGet(String id) {
        ValidateSyncTask task = resolveTask(id);
        Assert.notNull(task, "The task id is invalid.");
        task.setSourceTable(updateConnectorTables(task, ConnectorInstanceUtil.SOURCE_SUFFIX));
        task.setTargetTable(updateConnectorTables(task, ConnectorInstanceUtil.TARGET_SUFFIX));
        taskService.edit(task);
        return task;
    }

    @Override
    public String refreshFields(String id) {
        TableGroup tableGroup = tableGroupProfile.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");

        ValidateSyncTask task = resolveTask(tableGroup.getTaskId());
        Assert.notNull(task, "The task id is invalid.");
        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();
        List<String> sourceTablePks = sourceTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        List<String> targetTablePks = targetTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        validateSyncTableGroupChecker.updateTableColumn(task, ConnectorInstanceUtil.SOURCE_SUFFIX, StringUtil.join(sourceTablePks, ","), sourceTable);
        validateSyncTableGroupChecker.updateTableColumn(task, ConnectorInstanceUtil.TARGET_SUFFIX, StringUtil.join(targetTablePks, ","), targetTable);
        taskService.edit(task);
        return id;
    }

    @Override
    public String addTableGroup(Map<String, String> params) {
        String taskId = params.get("taskId");
        ValidateSyncTask task = resolveTask(taskId);
        assertRunning(task.getId());
        synchronized (LOCK) {
            try {
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
                    TableGroup model = (TableGroup) validateSyncTableGroupChecker.checkAddConfigModel(params);
                    validateSyncTableGroupChecker.mergeConfig(task, model);
                    log(LogType.TableGroupLog.INSERT, task, model);
                    int tableGroupCount = tableGroupProfile.getTableGroupCount(taskId);
                    model.setIndex(tableGroupCount + 1);
                    id = tableGroupProfile.addTableGroup(model);
                    list.add(id);
                }
                // 合并任务公共字段
                mergeTaskColumn(task);
                return 1 < tableSize ? String.valueOf(tableSize) : id;
            } finally {
                // 表映射变更后对齐明细
                validateSyncDetailService.syncTaskTableMetaDetails(taskId);
            }
        }
    }

    @Override
    public String editTableGroup(Map<String, String> params) {
        String tableGroupId = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = tableGroupProfile.getTableGroup(tableGroupId);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        ValidateSyncTask task = resolveTask(tableGroup.getTaskId());
        assertRunning(task.getId());

        TableGroup model = (TableGroup) validateSyncTableGroupChecker.checkEditConfigModel(params);
        validateSyncTableGroupChecker.mergeConfig(task, model);
        log(LogType.TableGroupLog.UPDATE, task, tableGroup);
        tableGroupProfile.editTableGroup(model);
        return tableGroupId;
    }

    @Override
    public String removeTableGroup(String taskId, String ids) {
        Assert.hasText(taskId, "Task id can not be null");
        Assert.hasText(ids, "TableGroup ids can not be null");
        ValidateSyncTask task = resolveTask(taskId);
        assertRunning(taskId);
        // 批量删除表
        Stream.of(StringUtil.split(ids, ",")).parallel().forEach(id -> {
            TableGroup model = tableGroupProfile.getTableGroup(id);
            log(LogType.TableGroupLog.DELETE, task, model);
            tableGroupProfile.removeTableGroup(id);
        });
        // 合并任务公共字段
        mergeTaskColumn(task);
        // 重置排序
        resetTableGroupAllIndex(taskId);
        // 对齐删除已无表映射的明细
        validateSyncDetailService.syncTaskTableMetaDetails(taskId);
        return taskId;
    }

    public List<Table> updateConnectorTables(ValidateSyncTask task, String suffix) {
        boolean isSource = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(task, isSource);
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(context.getMappingId(), context.getConnectorId(), context.getSuffix());
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        List<Table> tables = connectorFactory.getTables(connectorInstance, context);
        // 按升序展示表
        Collections.sort(tables, Comparator.comparing(Table::getName));
        return tables;
    }

    private void resetTableGroupAllIndex(String taskId) {
        synchronized (LOCK) {
            List<String> orderedIds = new ArrayList<>();
            tableGroupProfile.pageScanTableGroups(taskId, ConfigConstant.PAGE_SIZE, page -> {
                for (TableGroup g : page) {
                    if (g != null && StringUtil.isNotBlank(g.getId())) {
                        orderedIds.add(g.getId());
                    }
                }
            });
            int i = orderedIds.size();
            for (String groupId : orderedIds) {
                TableGroup g = tableGroupProfile.getTableGroup(groupId);
                if (g == null) {
                    continue;
                }
                g.setIndex(i--);
                profileComponent.editConfigModel(g);
            }
        }
    }

    private ValidateSyncTaskVO convertTask2Vo(ConfigModel task) {
        if (task == null) {
            return null;
        }

        ValidateSyncTask validateSyncTask = (ValidateSyncTask) task;
        Connector s = profileComponent.getConnector(validateSyncTask.getSourceConnectorId());
        Connector t = profileComponent.getConnector(validateSyncTask.getTargetConnectorId());
        ValidateSyncTaskVO vo = new ValidateSyncTaskVO(s, t);
        BeanUtils.copyProperties(task, vo);
        Meta taskMeta = metaProfile.getMetaByTaskId(validateSyncTask.getId(), TaskLevelEnum.TASK);
        if (taskMeta != null) {
            vo.setMetaState(taskMeta.getState());
            vo.setBeginTime(taskMeta.getBeginTime() > 0 ? taskMeta.getBeginTime() : null);
            vo.setEndTime(taskMeta.getEndTime() > 0 ? taskMeta.getEndTime() : null);
        }
        return vo;
    }

    private void checkTask(ValidateSyncTask task, Map<String, String> params) {
        if (StringUtil.isBlank(task.getId())) {
            task.setId(String.valueOf(snowflakeIdWorker.nextId()));
            task.setType(CommonTaskTypeEnum.VALIDATE_SYNC.name());
        }
        long now = Instant.now().toEpochMilli();
        task.setCreateTime(null == task.getCreateTime() ? now : task.getCreateTime());
        task.setUpdateTime(now);
        task.setName(params.get("name"));
        // 过滤条件
        String filterJson = params.get("filter");
        if (StringUtil.isNotBlank(filterJson)) {
            List<Filter> list = JsonUtil.jsonToArray(filterJson, Filter.class);
            task.setFilter(list);
        }

        String trigger = params.get("trigger");
        String cron = params.get("cron");
        if (StringUtil.isNotBlank(trigger)) {
            CommonTaskTriggerEnum type = CommonTaskTriggerEnum.getType(trigger);
            Assert.notNull(type, "trigger is not valid");
            task.setTrigger(type.getCode());
        }
        if (StringUtil.isNotBlank(cron)) {
            task.setCron(cron);
        }
        task.setEnableSync(StringUtil.isNotBlank(params.get("enableSync")));
        task.setEnableReverseScan(StringUtil.isNotBlank(params.get("enableReverseScan")));
        task.setEnableReverseSync(StringUtil.isNotBlank(params.get("enableReverseSync")));
        task.setEnableSchema(StringUtil.isNotBlank(params.get("enableSchema")));
        task.setEnableRowData(StringUtil.isNotBlank(params.get("enableRowData")));
        // 统一前置条件：行数据未开启，则反向扫描、反向同步全部禁用
        if (!task.isEnableRowData()) {
            task.setEnableReverseScan(false);
            task.setEnableReverseSync(false);
        } else if (!task.isEnableReverseScan()) {
            // 行数据开启，但反向扫描关闭 → 同步关闭反向订正
            task.setEnableReverseSync(false);
        }
        task.setEnableIndex(StringUtil.isNotBlank(params.get("enableIndex")));
        task.setEnableTrigger(StringUtil.isNotBlank(params.get("enableTrigger")));
        task.setEnableFunction(StringUtil.isNotBlank(params.get("enableFunction")));
        task.setReadNum(NumberUtil.toInt(params.get("readNum"), task.getReadNum()));
        task.setBatchNum(NumberUtil.toInt(params.get("batchNum"), task.getBatchNum()));
        task.setThreadNum(NumberUtil.toInt(params.get("threadNum"), task.getThreadNum()));
    }

    private void log(LogType log, ValidateSyncTask task, TableGroup tableGroup) {
        if (null != task) {
            // 新增订正校验任务知识库(执行一次)映射关系:[My_User] >> [My_User_Target]
            String name = task.getName();
            CommonTaskTriggerEnum type = CommonTaskTriggerEnum.getType(task.getTrigger());
            String s = tableGroup.getSourceTable().getName();
            String t = tableGroup.getTargetTable().getName();
            logService.log(log, "%s订正校验任务%s(%s)%s:[%s] >> [%s]", log.getMessage(), name, type.getMessage(), log.getName(), s, t);
        }
    }

    private void mergeTaskColumn(ValidateSyncTask task) {
        List<Field> sourceColumn = null;
        final List<Field>[] holder = new List[]{sourceColumn};
        tableGroupProfile.pageScanTableGroups(task.getId(), ConfigConstant.PAGE_SIZE, groups -> {
            for (TableGroup g : groups) {
                if (g == null || g.getSourceTable() == null || CollectionUtils.isEmpty(g.getSourceTable().getColumn())) {
                    continue;
                }
                holder[0] = PickerUtil.pickCommonFields(holder[0], g.getSourceTable().getColumn());
            }
        });
        task.setSourceColumn(holder[0]);
    }

    private List<CommonTaskSnapshot> collectValidateTableSnapshots(String taskId) {
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
     * 已完成表数：明细 Meta 快照中 status=已完成 的个数。
     */
    private int countCompletedTables(boolean roundDone, int totalSize, List<CommonTaskSnapshot> tableSnapshots) {
        if (roundDone) {
            return totalSize;
        }
        if (CollectionUtils.isEmpty(tableSnapshots) || totalSize <= 0) {
            return 0;
        }
        long doneCount = tableSnapshots.stream()
                .filter(snapshot -> snapshot != null && CommonTaskStatusEnum.isDone(snapshot.getStatus()))
                .count();
        return (int) Math.min(doneCount, totalSize);
    }

    /**
     * 进度百分比：completed / 表总数 * 100。
     */
    private BigDecimal calculateProgressPercent(boolean roundDone, int totalSize, List<CommonTaskSnapshot> tableSnapshots) {
        if (roundDone) {
            return new BigDecimal("100.00");
        }
        if (totalSize <= 0) {
            return BigDecimal.ZERO;
        }
        int completed = countCompletedTables(false, totalSize, tableSnapshots);
        return BigDecimal.valueOf(completed)
                .multiply(BigDecimal.valueOf(100))
                .divide(BigDecimal.valueOf(totalSize), 2, RoundingMode.HALF_UP);
    }

    protected void assertRunning(String taskId) {
        synchronized (LOCK) {
            Assert.isTrue(!taskService.isRunning(taskId), "任务正在执行, 请先停止.");
        }
    }

}
