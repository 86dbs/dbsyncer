/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.RepeatedTableGroupException;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.impl.mapping.MappingChecker;
import org.dbsyncer.biz.task.MappingCountTask;
import org.dbsyncer.biz.vo.MappingCustomTableVO;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.biz.vo.TableVO;
import org.dbsyncer.common.dispatch.DispatchTaskService;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class MappingServiceImpl extends BaseServiceImpl implements MappingService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private MappingChecker mappingChecker;

    @Resource
    private TableGroupService tableGroupService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MonitorService monitorService;

    @Resource
    private DispatchTaskService dispatchTaskService;

    @Resource
    private ManagerFactory managerFactory;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private TableGroupContext tableGroupContext;

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private PreloadTemplate preloadTemplate;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = mappingChecker.checkAddConfigModel(params);
        log(LogType.MappingLog.INSERT, model);

        String id = profileComponent.addConfigModel(model);
        // 加载驱动表
        refreshMappingTables(id);

        // 匹配相似表 on
        if (StringUtil.isNotBlank(params.get("autoMatchTable"))) {
            matchSimilarTableGroups(model);
            return id;
        }

        // 自定义表映射关系
        String tableGroups = params.get("tableGroups");
        if (StringUtil.isNotBlank(tableGroups)) {
            matchCustomizedTableGroups(model, tableGroups);
        }
        // 统计总数
        submitMappingCountTask((Mapping) model, null);
        return id;
    }

    @Override
    public String copy(String id) {
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "The mapping id is invalid.");

        String json = JsonUtil.objToJson(mapping);
        Mapping newMapping = JsonUtil.jsonToObj(json, Mapping.class);
        newMapping.setName(mapping.getName() + "(复制)");
        newMapping.setId(String.valueOf(snowflakeIdWorker.nextId()));
        newMapping.setUpdateTime(Instant.now().toEpochMilli());
        mappingChecker.addMeta(newMapping);

        profileComponent.addConfigModel(newMapping);
        preloadTemplate.reConnect(newMapping);
        log(LogType.MappingLog.COPY, newMapping);

        // 复制映射表关系
        List<TableGroup> groupList = profileComponent.getTableGroupAll(mapping.getId());
        if (!CollectionUtils.isEmpty(groupList)) {
            groupList.forEach(tableGroup -> {
                String tableGroupJson = JsonUtil.objToJson(tableGroup);
                TableGroup newTableGroup = JsonUtil.jsonToObj(tableGroupJson, TableGroup.class);
                newTableGroup.setId(String.valueOf(snowflakeIdWorker.nextId()));
                newTableGroup.setMappingId(newMapping.getId());
                profileComponent.addTableGroup(newTableGroup);
                log(LogType.TableGroupLog.COPY, newTableGroup);
            });
        }
        return String.format("复制成功[%s]", newMapping.getName());
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        String metaSnapshot = params.get("metaSnapshot");
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            Mapping model = (Mapping) mappingChecker.checkEditConfigModel(params);
            log(LogType.MappingLog.UPDATE, model);

            // 更新meta
            tableGroupService.updateMeta(mapping, metaSnapshot);
            profileComponent.editConfigModel(model);
        }
        // 统计总数
        submitMappingCountTask(mapping, metaSnapshot);
        return id;
    }

    @Override
    public String remove(String id) {
        Mapping mapping = assertMappingExist(id);
        String metaId = mapping.getMetaId();
        Meta meta = profileComponent.getMeta(metaId);
        synchronized (LOCK) {
            assertRunning(metaId);

            // 删除数据
            monitorService.clearData(metaId);
            log(LogType.MetaLog.CLEAR, meta);

            // 删除meta
            profileComponent.removeConfigModel(metaId);
            log(LogType.MetaLog.DELETE, meta);

            // 删除tableGroup
            List<TableGroup> groupList = profileComponent.getTableGroupAll(id);
            if (!CollectionUtils.isEmpty(groupList)) {
                groupList.forEach(t -> profileComponent.removeTableGroup(t.getId()));
            }

            // 删除驱动表映射关系
            tableGroupContext.clear(metaId);

            // 删除驱动
            profileComponent.removeConfigModel(id);
            log(LogType.MappingLog.DELETE, mapping);
        }
        return "驱动删除成功";
    }

    @Override
    public MappingVo getMapping(String id) {
        Mapping mapping = profileComponent.getMapping(id);
        return convertMapping2Vo(mapping);
    }

    @Override
    public MappingCustomTableVO getMappingCustomTable(String id, String type) {
        Mapping mapping = profileComponent.getMapping(id);
        MappingCustomTableVO vo = new MappingCustomTableVO();
        vo.setId(mapping.getId());
        vo.setName(mapping.getName());
        boolean isSource = StringUtil.equals("source", type);
        List<Table> tables = isSource ? mapping.getSourceTable() : mapping.getTargetTable();
        String connectorId = isSource ? mapping.getSourceConnectorId() : mapping.getTargetConnectorId();
        ConnectorConfig config = profileComponent.getConnector(connectorId).getConfig();
        ConnectorService<?,?> connectorService = connectorFactory.getConnectorService(config);
        vo.setConnectorType(connectorService.getConnectorType());
        vo.setExtendedType(connectorService.getExtendedTableType().getCode());

        // 只返回自定义表类型
        if (!CollectionUtils.isEmpty(tables)) {
            List<Table> mainTables = new ArrayList<>();
            List<TableVO> customTables = new ArrayList<>();
            tables.forEach(t -> {
                switch (TableTypeEnum.getTableType(t.getType())) {
                    case SQL:
                    case SEMI:
                        TableVO tableVO = new TableVO();
                        BeanUtils.copyProperties(t, tableVO);
                        customTables.add(tableVO);
                        break;
                    case TABLE:
                        mainTables.add(t);
                    default:
                        break;
                }
            });
            vo.setCustomTables(customTables.stream().sorted(Comparator.comparing(Table::getName)).collect(Collectors.toList()));
            vo.setMainTables(mainTables.stream().sorted(Comparator.comparing(Table::getName)).collect(Collectors.toList()));
        }
        // 元信息
        vo.setMeta(profileComponent.getMeta(mapping.getMetaId()));
        return vo;
    }

    @Override
    public MappingVo getMapping(String id, Integer exclude) {
        Mapping mapping = profileComponent.getMapping(id);
        // 显示所有表
        if (exclude != null && exclude == 1) {
            return convertMapping2Vo(mapping);
        }
        // 过滤已映射的表
        MappingVo vo = convertMapping2Vo(mapping);
        List<TableGroup> tableGroupAll = tableGroupService.getTableGroupAll(id);
        if (!CollectionUtils.isEmpty(tableGroupAll)) {
            final Set<String> sTables = new HashSet<>();
            final Set<String> tTables = new HashSet<>();
            tableGroupAll.forEach(tableGroup -> {
                sTables.add(tableGroup.getSourceTable().getName());
                tTables.add(tableGroup.getTargetTable().getName());
            });
            vo.setSourceTable(mapping.getSourceTable().stream().filter(t -> !CollectionUtils.isEmpty(sTables) && !sTables.contains(t.getName())).collect(Collectors.toList()));
            vo.setTargetTable(mapping.getTargetTable().stream().filter(t -> !CollectionUtils.isEmpty(tTables) && !tTables.contains(t.getName())).collect(Collectors.toList()));
            sTables.clear();
            tTables.clear();
        }
        return vo;
    }

    @Override
    public List<MappingVo> getMappingAll() {
        return profileComponent.getMappingAll()
                .stream()
                .map(this::convertMapping2Vo)
                .sorted(Comparator.comparing(MappingVo::getUpdateTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public Paging<MappingVo> search(Map<String, String> params) {
        return searchConfigModel(params, getMappingAll());
    }

    @Override
    public String start(String id) {
        Mapping mapping = assertMappingExist(id);
        final String metaId = mapping.getMetaId();
        // 如果已经完成了，重置状态
        clearMetaIfFinished(metaId);

        synchronized (LOCK) {
            assertRunning(metaId);

            // 启动
            managerFactory.start(mapping);

            log(LogType.MappingLog.RUNNING, mapping);
        }
        return "驱动启动成功";
    }

    @Override
    public String stop(String id) {
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            if (!isRunning(mapping.getMetaId())) {
                throw new BizException("驱动已停止.");
            }
            managerFactory.close(mapping);

            log(LogType.MappingLog.STOP, mapping);

            // 发送关闭驱动通知消息
            String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
            sendNotifyMessage("停止驱动", String.format("手动停止驱动：%s(%s)", mapping.getName(), model));
        }
        return "驱动停止成功";
    }

    @Override
    public String refreshMappingTables(String id) {
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "The mapping id is invalid.");
        mapping.setSourceTable(updateConnectorTables(mapping, ConnectorInstanceUtil.SOURCE_SUFFIX));
        mapping.setTargetTable(updateConnectorTables(mapping, ConnectorInstanceUtil.TARGET_SUFFIX));
        profileComponent.editConfigModel(mapping);
        return "刷新驱动表成功";
    }

    @Override
    public Table getCustomTable(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String customTable = params.get("customTable");
        boolean isSource = StringUtil.equals("source", type);
        List<Table> tables = getMappingTables(mapping, isSource);
        if (!CollectionUtils.isEmpty(tables)) {
            for (Table t : tables) {
                if (StringUtil.equals(t.getName(), customTable)) {
                    return t;
                }
            }
        }
        return null;
    }

    @Override
    public String saveCustomTable(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            saveCustomTable(mapping, params);
            profileComponent.editConfigModel(mapping);
            log(LogType.MappingLog.UPDATE, mapping);
        }
        return id;
    }

    @Override
    public String removeCustomTable(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            removeCustomTable(mapping, params);
            profileComponent.editConfigModel(mapping);
            log(LogType.MappingLog.UPDATE, mapping);
        }
        return id;
    }

    /**
     * 提交统计驱动总数任务
     */
    private void submitMappingCountTask(Mapping mapping, String metaSnapshot) {
        MappingCountTask task = new MappingCountTask();
        task.setMappingId(mapping.getId());
        task.setMetaSnapshot(metaSnapshot);
        task.setParserComponent(parserComponent);
        task.setProfileComponent(profileComponent);
        task.setTableGroupService(tableGroupService);
        task.setConnectorFactory(connectorFactory);
        dispatchTaskService.execute(task);
    }

    private List<Table> updateConnectorTables(Mapping mapping, String suffix) {
        boolean isSource = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(mapping, isSource);

        // 合并自定义表
        List<Table> customTables = new ArrayList<>();
        List<Table> tables = getMappingTables(mapping, isSource);
        tables.forEach(t -> {
            switch (TableTypeEnum.getTableType(t.getType())) {
                case SQL:
                case SEMI:
                    customTables.add(t);
                    break;
                default:
                    break;
            }
        });

        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(context.getMappingId(), context.getConnectorId(), context.getSuffix());
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        tables = connectorFactory.getTables(connectorInstance, context);
        tables.addAll(customTables);
        // 按升序展示表
        Collections.sort(tables, Comparator.comparing(Table::getName));
        return tables;
    }

    private MappingVo convertMapping2Vo(Mapping mapping) {
        String model = mapping.getModel();
        Assert.notNull(mapping, "Mapping can not be null.");

        // 元信息
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "Meta can not be null.");
        MetaVo metaVo = new MetaVo(ModelEnum.getModelEnum(model).getName(), mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);
        metaVo.setCounting(dispatchTaskService.isRunning(mapping.getId()));

        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        MappingVo vo = new MappingVo(s, t, metaVo);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }

    /**
     * 检查是否存在驱动
     *
     * @param mappingId
     * @return
     */
    private Mapping assertMappingExist(String mappingId) {
        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "驱动不存在.");
        return mapping;
    }

    /**
     * 匹配相似表
     *
     * @param model
     */
    private void matchSimilarTableGroups(ConfigModel model) {
        Mapping mapping = (Mapping) model;
        List<Table> sourceTables = mapping.getSourceTable();
        List<Table> targetTables = mapping.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            return;
        }
        // 优化匹配性能
        Map<String, Table> targetTableMap = targetTables.stream().collect(Collectors.toMap(table -> table.getName().toUpperCase(), table -> table));

        // 匹配相似表
        for (Table sourceTable : sourceTables) {
            if (StringUtil.isBlank(sourceTable.getName())) {
                continue;
            }
            targetTableMap.computeIfPresent(sourceTable.getName().toUpperCase(), (k, targetTable) -> {
                // 仅支持表类型
                if (TableTypeEnum.isTable(targetTable.getType())) {
                    addTableGroup(mapping.getId(), sourceTable.getName(), targetTable.getName(), StringUtil.EMPTY);
                }
                return targetTable;
            });
        }
    }

    /**
     * 自定义配置表映射关系
     *
     * @param model
     * @param tableGroups
     */
    private void matchCustomizedTableGroups(ConfigModel model, String tableGroups) {
        Mapping mapping = (Mapping) model;
        List<Table> sourceTables = mapping.getSourceTable();
        List<Table> targetTables = mapping.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            return;
        }
        String[] lines = StringUtil.split(tableGroups, StringUtil.BREAK_LINE);
        // 数据源表|目标源表=源表字段A1*|目标字段A2*
        for (String line : lines) {
            String[] tableGroup = StringUtil.split(line, StringUtil.EQUAL);
            String[] tableGroupNames = StringUtil.split(tableGroup[0], StringUtil.VERTICAL_LINE);
            if (tableGroupNames.length == 2) {
                addTableGroup(mapping.getId(), tableGroupNames[0], tableGroupNames[1], tableGroup.length == 2 ? tableGroup[1] : StringUtil.EMPTY);
            }
        }
    }

    private void addTableGroup(String id, String sourceTableName, String targetTableName, String fieldMappings) {
        try {
            Map<String, String> params = new HashMap<>();
            params.put("mappingId", id);
            params.put("sourceTable", sourceTableName);
            params.put("targetTable", targetTableName);
            // A1*|A2*,B1|B2,|C3
            if (StringUtil.isNotBlank(fieldMappings)) {
                String[] mappings = StringUtil.split(fieldMappings, StringUtil.COMMA);
                StringJoiner fms = new StringJoiner(StringUtil.COMMA);
                StringJoiner sPk = new StringJoiner(StringUtil.COMMA);
                StringJoiner tPk = new StringJoiner(StringUtil.COMMA);
                for (String mapping : mappings) {
                    String[] m = StringUtil.split(mapping, "\\" + StringUtil.VERTICAL_LINE);
                    if (m.length == 2) {
                        fms.add(replaceStar(m[0], sPk) + StringUtil.VERTICAL_LINE + replaceStar(m[1], tPk));
                        continue;
                    }
                    // |C2,C3|
                    if (m.length == 1) {
                        String name = replaceStar(m[0], tPk);
                        if (StringUtil.startsWith(mapping, StringUtil.VERTICAL_LINE)) {
                            fms.add(StringUtil.VERTICAL_LINE + name);
                            continue;
                        }
                        fms.add(name + StringUtil.VERTICAL_LINE);
                    }
                }
                params.put("fieldMappings", fms.toString());
                if (StringUtil.isNotBlank(sPk.toString())) {
                    params.put("sourceTablePK", sPk.toString());
                }
                if (StringUtil.isNotBlank(tPk.toString())) {
                    params.put("targetTablePK", tPk.toString());
                }
            }
            tableGroupService.add(params);
        } catch (RepeatedTableGroupException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private String replaceStar(String name, StringJoiner joiner) {
        if (StringUtil.endsWith(name, StringUtil.STAR)) {
            name = StringUtil.replace(name.trim(), StringUtil.STAR, StringUtil.EMPTY);
            joiner.add(name);
        }
        return name;
    }

    private void clearMetaIfFinished(String metaId) {
        Meta meta = profileComponent.getMeta(metaId);
        Assert.notNull(meta, "Mapping meta can not be null.");
        // 完成任务则重置状态
        if (meta.getTotal().get() <= (meta.getSuccess().get() + meta.getFail().get())) {
            meta.getFail().set(0);
            meta.getSuccess().set(0);
            profileComponent.editConfigModel(meta);
        }
    }

    private void saveCustomTable(Mapping mapping, Map<String, String> params) {
        String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String operator = params.get("operator");
        String customTable = params.get("customTable");
        boolean isSource = StringUtil.equals("source", type);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(mapping, isSource);

        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(context.getMappingId(), context.getConnectorId(), context.getSuffix());
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        ConnectorService connectorService = connectorFactory.getConnectorService(connectorInstance.getConfig());
        ConfigValidator configValidator = connectorService.getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        Table newTable = configValidator.modifyExtendedTable(connectorService, params);
        Assert.notNull(newTable, "解析自定义表异常");

        context.addTablePattern(newTable);
        List<MetaInfo> metaInfos = connectorService.getMetaInfo(connectorInstance, context);
        Assert.notEmpty(metaInfos, "执行SQL异常");
        Assert.notEmpty(metaInfos.get(0).getColumn(), "获取字段信息异常");

        List<Table> tables = getMappingTables(mapping, isSource);
        // 首次添加
        if (CollectionUtils.isEmpty(tables)) {
            tables.add(newTable);
            return;
        }

        // 新增操作
        Set<String> exist = tables.stream().map(Table::getName).collect(Collectors.toSet());
        String newTableName = newTable.getName();
        if (StringUtil.equals("add", operator)) {
            if (exist.contains(newTableName)) {
                throw new BizException(String.format("%s或自定义表名重复，请更换", isSource ? "数据源" : "目标源"));
            }
            tables.add(newTable);
            // 按升序展示表
            Collections.sort(tables, Comparator.comparing(Table::getName));
            return;
        }

        // 修改操作，更改表名
        if (!StringUtil.equals(customTable, newTableName)) {
            if (exist.contains(newTableName)) {
                throw new BizException(String.format("%s或自定义表名重复，请更换", isSource ? "数据源" : "目标源"));
            }
        }
        for (Table t : tables) {
            if (StringUtil.equals(t.getName(), customTable)) {
                t.setName(newTable.getName());
                t.setColumn(newTable.getColumn());
                t.setExtInfo(newTable.getExtInfo());
                break;
            }
        }

        // 按升序展示表
        Collections.sort(tables, Comparator.comparing(Table::getName));
    }

    private List<Table> getMappingTables(Mapping mapping, boolean isSource) {
        mapping.setSourceTable(!CollectionUtils.isEmpty(mapping.getSourceTable()) ? mapping.getSourceTable() : new ArrayList<>());
        mapping.setTargetTable(!CollectionUtils.isEmpty(mapping.getTargetTable()) ? mapping.getTargetTable() : new ArrayList<>());
        return isSource ? mapping.getSourceTable() : mapping.getTargetTable();
    }

    private void removeCustomTable(Mapping mapping, Map<String, String> params) {
        String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String tableName = params.get("customTable");
        Assert.hasText(tableName, "无自定义表.");
        List<Table> tables = StringUtil.equals("source", type) ? mapping.getSourceTable() : mapping.getTargetTable();
        if (!CollectionUtils.isEmpty(tables)) {
            Iterator<Table> iterator = tables.iterator();
            while (iterator.hasNext()) {
                Table t = iterator.next();
                switch (TableTypeEnum.getTableType(t.getType())) {
                    case SQL:
                    case SEMI:
                        if (StringUtil.equals(t.getName(), tableName)) {
                            iterator.remove();
                            return;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

}