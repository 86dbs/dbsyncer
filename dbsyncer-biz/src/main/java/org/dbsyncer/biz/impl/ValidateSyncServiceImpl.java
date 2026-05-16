/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.biz.checker.impl.mapping.MappingChecker;
import org.dbsyncer.biz.checker.impl.tablegroup.ValidateSyncTableGroupChecker;
import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.CommonTaskTriggerEnum;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.*;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.sdk.storage.StorageService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class ValidateSyncServiceImpl implements ValidateSyncService {

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private TaskService<ValidateSyncTask> taskService;

    @Resource
    private StorageService storageService;

    @Resource
    private ProfileComponent profileComponent;

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

    /**
     * 任务启停锁
     */
    private final static Object LOCK = new Object();

    @Override
    public ValidateSyncTaskVO get(String id) {
        return convertTask2Vo(taskService.get(id));
    }

    @Override
    public String add(Map<String, String> params) {
        ValidateSyncTask task = new ValidateSyncTask();
        checkTask(task, params);
        // 默认检查行数据
        task.setEnablerRowData(true);
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
            List<TableGroup> tableGroupAll = tableGroupService.getTableGroupAll(mappingId);
            if (!CollectionUtils.isEmpty(tableGroupAll)) {
                tableGroupAll.forEach(tableGroup -> {
                    TableGroup newTable = deepCopy(tableGroup);
                    newTable.setId(String.valueOf(snowflakeIdWorker.nextId()));
                    newTable.setMappingId(task.getId());
                    profileComponent.addTableGroup(newTable);
                });
            }
            // 合并任务公共字段
            mergeTaskColumn(task);
            String id = taskService.add(task);
            preloadTemplate.reConnect(task);
            return id;
        } else {
            task.setSourceConnectorId(params.get("sourceConnectorId"));
            task.setSourceDatabase(params.get("sourceDatabase"));
            task.setSourceSchema(params.get("sourceSchema"));
            task.setTargetConnectorId(params.get("targetConnectorId"));
            task.setTargetDatabase(params.get("targetDatabase"));
            task.setTargetSchema(params.get("targetSchema"));
            // 先持久化再建连 才能拉去到所有表
            String id = taskService.add(task);
            preloadTemplate.reConnect(task);
            refreshTables(id);
            ValidateSyncTask validateSyncTask = taskService.get(id);
            // 与 MappingServiceImpl 一致：勾选「匹配相似表」时仅走自动匹配，否则解析自定义表映射文本
            if (StringUtil.isNotBlank(params.get("autoMatchTable"))) {
                matchSimilarTableGroups(validateSyncTask);
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
     * 匹配相似表
     *
     */
    private void matchSimilarTableGroups(ValidateSyncTask validateSyncTask) {
        List<Table> sourceTables = validateSyncTask.getSourceTable();
        List<Table> targetTables = validateSyncTask.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            return;
        }
        // 同名目标表只保留首次出现，避免 toMap 在重名时抛异常
        Map<String, Table> targetTableMap = new LinkedHashMap<>();
        for (Table table : targetTables) {
            if (table == null || StringUtil.isBlank(table.getName())) {
                continue;
            }
            targetTableMap.putIfAbsent(table.getName().toUpperCase(Locale.ROOT), table);
        }

        for (Table sourceTable : sourceTables) {
            if (StringUtil.isBlank(sourceTable.getName())) {
                continue;
            }
            targetTableMap.computeIfPresent(sourceTable.getName().toUpperCase(Locale.ROOT), (k, targetTable) -> {
                if (TableTypeEnum.isTable(targetTable.getType())) {
                    Map<String, String> p = new HashMap<>();
                    p.put("taskId", validateSyncTask.getId());
                    p.put("sourceTable", sourceTable.getName());
                    p.put("targetTable", targetTable.getName());
                    // ValidateSyncTableGroupChecker 必填，与 Mapping 侧物理表映射一致
                    p.put("sourceType", StringUtil.isNotBlank(sourceTable.getType()) ? sourceTable.getType() : TableTypeEnum.TABLE.getCode());
                    p.put("targetType", StringUtil.isNotBlank(targetTable.getType()) ? targetTable.getType() : TableTypeEnum.TABLE.getCode());
                    addTableGroup(p);
                }
                return targetTable;
            });
        }
    }

    @Override
    public String edit(Map<String, String> params) {
        ValidateSyncTask task = taskService.get(params.get("id"));
        if (task == null) {
            throw new IllegalArgumentException("Task not found");
        }
        checkTask(task, params);
        resetTaskSnapshot(task);
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(task.getId());
        if (!CollectionUtils.isEmpty(groupAll)) {
            mappingChecker.sortTableGroup(groupAll, params);
            for (TableGroup g : groupAll) {
                validateSyncTableGroupChecker.mergeConfig(task, g);
                profileComponent.editConfigModel(g);
            }
        }
        return taskService.edit(task);
    }

    @Override
    public String copy(String id) {
        ValidateSyncTask task = taskService.get(id);
        Assert.notNull(task, "Task not found");
        String json = JsonUtil.objToJson(task);
        ValidateSyncTask newTask = JsonUtil.jsonToObj(json, ValidateSyncTask.class);
        newTask.setId(String.valueOf(snowflakeIdWorker.nextId()));
        newTask.setName(newTask.getName() + "(复制)");
        newTask.setStatus(CommonTaskStatusEnum.READY.getCode());
        newTask.setType(CommonTaskTypeEnum.VALIDATE_SYNC.name());
        newTask.setUpdateTime(System.currentTimeMillis());
        resetTaskSnapshot(newTask);
        String newId = taskService.add(newTask);
        preloadTemplate.reConnect(newTask);
        return newId;
    }

    @Override
    public String delete(String id) {
        assertRunning(id);
        List<TableGroup> groupList = profileComponent.getTableGroupAll(id);
        if (!CollectionUtils.isEmpty(groupList)) {
            groupList.forEach(t -> profileComponent.removeTableGroup(t.getId()));
        }
        taskService.delete(id);
        return "删除成功";
    }

    @Override
    public String start(String id) {
        List<TableGroup> tableGroupList = profileComponent.getSortedTableGroupAll(id);
        Assert.isTrue(!CollectionUtils.isEmpty(tableGroupList), "任务未配置表映射，无法启动");
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
                    long errorCount = countTaskDetail(t.getId());
                    vo.setErrorCount(errorCount);
                    List<TableGroup> tableGroupList = profileComponent.getSortedTableGroupAll(t.getId());
                    vo.setProgress(calculateProgressPercent(t, tableGroupList.size()));
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
        ValidateSyncTask task = taskService.get(id);
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

        ValidateSyncTask task = taskService.get(id);
        Assert.notNull(task, "task not found.");

        boolean isSource = !"target".equals(type);
        List<Table> tables = isSource ? task.getSourceTable() : task.getTargetTable();
        tables = CollectionUtils.isEmpty(tables) ? Collections.emptyList() : tables;

        // 已映射/已配置的表名
        Set<String> mappedTableNames;
        if (excludeMapped) {
            mappedTableNames = tableGroupService.getTableGroupAll(id).stream()
                    .map(g -> isSource ? g.getSourceTable() : g.getTargetTable())
                    .filter(Objects::nonNull)
                    .map(Table::getName)
                    .filter(StringUtil::isNotBlank)
                    .map(n -> n.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toSet());
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
        return taskService.get(id);
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
        Assert.hasText(taskId, "taskId is required.");
        Query query = new Query(NumberUtil.toInt(params.get("pageNum"), 1), NumberUtil.toInt(params.get("pageSize"), 10));
        query.setType(StorageEnum.VALIDATE_SYNC_DETAIL);
        query.addFilter(ConfigConstant.TASK_ID, taskId);

        Set<String> selectFiled = getTaskDetailSelect();

        query.setSelectFlied(selectFiled);
        query.addOrderBy(ConfigConstant.TASK_DIFF_TOTAL, SortEnum.DESC);
        return storageService.query(query);
    }

    @Override
    public Object getValidateResultDetail(String id) {
        Assert.hasText(id, "id is required.");
        Query query = new Query(1, 1);
        query.setType(StorageEnum.VALIDATE_SYNC_DETAIL);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, id);
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        return paging.getData().iterator().next();
    }

    @Override
    public String refreshTables(String id) {
        ValidateSyncTask task = taskService.get(id);
        Assert.notNull(task, "The task id is invalid.");
        task.setSourceTable(updateConnectorTables(task, ConnectorInstanceUtil.SOURCE_SUFFIX));
        task.setTargetTable(updateConnectorTables(task, ConnectorInstanceUtil.TARGET_SUFFIX));
        taskService.edit(task);
        return id;
    }

    @Override
    public String refreshFields(String id) {
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");

        ValidateSyncTask task = taskService.get(tableGroup.getMappingId());
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
        ValidateSyncTask task = taskService.get(taskId);
        assertRunning(task.getId());
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
                TableGroup model = (TableGroup) validateSyncTableGroupChecker.checkAddConfigModel(params);
                validateSyncTableGroupChecker.mergeConfig(task, model);
                log(LogType.TableGroupLog.INSERT, task, model);
                int tableGroupCount = profileComponent.getTableGroupCount(taskId);
                model.setIndex(tableGroupCount + 1);
                id = profileComponent.addTableGroup(model);
                list.add(id);
            }
            // 合并任务公共字段
            mergeTaskColumn(task);
            return 1 < tableSize ? String.valueOf(tableSize) : id;
        }
    }

    @Override
    public String editTableGroup(Map<String, String> params) {
        String tableGroupId = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        ValidateSyncTask task = taskService.get(tableGroup.getMappingId());
        assertRunning(task.getId());

        TableGroup model = (TableGroup) validateSyncTableGroupChecker.checkEditConfigModel(params);
        validateSyncTableGroupChecker.mergeConfig(task, model);
        log(LogType.TableGroupLog.UPDATE, task, tableGroup);
        profileComponent.editTableGroup(model);
        return tableGroupId;
    }

    @Override
    public String removeTableGroup(String taskId, String ids) {
        Assert.hasText(taskId, "Task id can not be null");
        Assert.hasText(ids, "TableGroup ids can not be null");
        ValidateSyncTask task = taskService.get(taskId);
        assertRunning(taskId);
        // 批量删除表
        Stream.of(StringUtil.split(ids, ",")).parallel().forEach(id -> {
            TableGroup model = profileComponent.getTableGroup(id);
            log(LogType.TableGroupLog.DELETE, task, model);
            profileComponent.removeTableGroup(id);
        });
        // 合并任务公共字段
        mergeTaskColumn(task);
        // 重置排序
        resetTableGroupAllIndex(taskId);
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

    /**
     * 获取有异常的数据
     *
     * @param taskId
     * @return
     */
    private long countTaskDetail(String taskId) {
        Query query = new Query(1, 1);
        query.setType(StorageEnum.VALIDATE_SYNC_DETAIL);
        query.addFilter(ConfigConstant.TASK_ID, taskId);
        query.addFilter(ConfigConstant.TASK_DIFF_TOTAL, FilterEnum.GT, 0);
        query.setType(StorageEnum.VALIDATE_SYNC_DETAIL);
        query.setQueryTotal(true);
        Paging paging = storageService.query(query);
        return paging.getTotal();
    }

    private void resetTableGroupAllIndex(String taskId) {
        synchronized (LOCK) {
            List<TableGroup> list = profileComponent.getSortedTableGroupAll(taskId);
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

    private ValidateSyncTaskVO convertTask2Vo(CommonTask task) {
        if (task == null) {
            return null;
        }

        ValidateSyncTask validateSyncTask = (ValidateSyncTask) task;
        Connector s = profileComponent.getConnector(validateSyncTask.getSourceConnectorId());
        Connector t = profileComponent.getConnector(validateSyncTask.getTargetConnectorId());
        ValidateSyncTaskVO vo = new ValidateSyncTaskVO(s, t);
        BeanUtils.copyProperties(task, vo);
        return vo;
    }

    private void checkTask(ValidateSyncTask task, Map<String, String> params) {
        if (StringUtil.isBlank(task.getId())) {
            task.setId(String.valueOf(snowflakeIdWorker.nextId()));
            task.setStatus(CommonTaskStatusEnum.READY.getCode());
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
        task.setEnableSchema(StringUtil.isNotBlank(params.get("enableSchema")));
        task.setEnablerRowData(StringUtil.isNotBlank(params.get("enablerRowData")));
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
        List<TableGroup> groups = profileComponent.getTableGroupAll(task.getId());
        List<Field> sourceColumn = null;
        for (TableGroup g : groups) {
            sourceColumn = PickerUtil.pickCommonFields(sourceColumn, g.getSourceTable().getColumn());
        }
        task.setSourceColumn(sourceColumn);
    }

    /**
     * 进度百分比计算：
     * completed = (最小索引 - 1) + (快照中 status=1 的个数)
     * progress = completed / 表总数 * 100
     *
     * @param task      校验任务
     * @param totalSize 表总数
     * @return 百分比（0~100）
     */
    private BigDecimal calculateProgressPercent(ValidateSyncTask task, int totalSize) {

        if (task.getProcessed() == null) {
            return null;
        }
        if (task.getProcessed() != null && task.getProcessed() == 1) {
            return new BigDecimal("100.00");
        }
        if (task.getTableSnapshots().isEmpty()) {
            return BigDecimal.ZERO;
        }
        int minIndex = task.getTableSnapshots().keySet().stream()
                .min(Comparator.naturalOrder())  // 自然排序，取最小
                .orElse(0);

        long doneCount = task.getTableSnapshots().values().stream()
                .filter(snapshot -> snapshot != null && snapshot.getStatus() == 1)
                .count();
        long completed = Math.max(0, minIndex - 1L) + doneCount;
        if (completed > totalSize) {
            completed = totalSize;
        }
        // 公式：completed * 100 / totalSize 保留2位小数
        return BigDecimal.valueOf(completed)
                .multiply(BigDecimal.valueOf(100))
                .divide(BigDecimal.valueOf(totalSize), 2, RoundingMode.HALF_UP);
    }

    private void resetTaskSnapshot(ValidateSyncTask task) {
        if (task == null) {
            return;
        }
        task.setProcessed(0);
        task.getTableSnapshots().clear();
    }

    protected void assertRunning(String taskId) {
        synchronized (LOCK) {
            Assert.isTrue(!taskService.isRunning(taskId), "任务正在执行, 请先停止.");
        }
    }

    private static Set<String> getTaskDetailSelect() {
        Set<String> selectFiled = new HashSet<>();
        selectFiled.add(ConfigConstant.TASK_ID);
        selectFiled.add(ConfigConstant.CONFIG_MODEL_ID);
        selectFiled.add(ConfigConstant.TASK_SOURCE_TABLE_NAME);
        selectFiled.add(ConfigConstant.DATA_TARGET_TABLE_NAME);
        selectFiled.add(ConfigConstant.TASK_SOURCE_TOTAL);
        selectFiled.add(ConfigConstant.TASK_TARGET_TOTAL);
        selectFiled.add(ConfigConstant.TASK_DIFF_TOTAL);
        selectFiled.add(ConfigConstant.TASK_FIXED_TOTAL);
        selectFiled.add(ConfigConstant.CONFIG_MODEL_TYPE);
        selectFiled.add(ConfigConstant.CONFIG_MODEL_UPDATE_TIME);
        selectFiled.add(ConfigConstant.CONFIG_MODEL_CREATE_TIME);
        return selectFiled;
    }
}
