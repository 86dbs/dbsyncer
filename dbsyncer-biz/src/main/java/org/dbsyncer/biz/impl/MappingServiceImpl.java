/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.RepeatedTableGroupException;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.TaskSchedulerService;
import org.dbsyncer.biz.checker.impl.mapping.MappingChecker;
import org.dbsyncer.biz.scheduler.mapping.MappingCountTask;
import org.dbsyncer.biz.vo.ConnectorVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TableGroupContext;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
    private ConnectorService connectorService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MonitorService monitorService;

    @Resource
    private TaskSchedulerService taskSchedulerService;

    @Resource
    private ManagerFactory managerFactory;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private TableGroupContext tableGroupContext;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = mappingChecker.checkAddConfigModel(params);
        log(LogType.MappingLog.INSERT, model);

        String id = profileComponent.addConfigModel(model);

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
        submitMappingCountTask(id);
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
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            Mapping model = (Mapping) mappingChecker.checkEditConfigModel(params);
            log(LogType.MappingLog.UPDATE, model);

            // 更新meta
            tableGroupService.updateMeta(mapping, params.get("metaSnapshot"));
            profileComponent.editConfigModel(model);
        }
        // 统计总数
        submitMappingCountTask(id);
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
            vo.getSourceConnector().setTable(vo.getSourceConnector().getTable().stream().filter(t -> !sTables.contains(t.getName())).collect(Collectors.toList()));
            vo.getTargetConnector().setTable(vo.getTargetConnector().getTable().stream().filter(t -> !tTables.contains(t.getName())).collect(Collectors.toList()));
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
        updateConnectorTables(mapping.getSourceConnectorId());
        updateConnectorTables(mapping.getTargetConnectorId());
        return "刷新驱动表成功";
    }

    /**
     * 提交统计驱动总数任务
     */
    private void submitMappingCountTask(String id) {
        taskSchedulerService.submit(new MappingCountTask(id));
    }

    private void updateConnectorTables(String connectorId) {
        Connector connector = profileComponent.getConnector(connectorId);
        Assert.notNull(connector, "The connector id is invalid.");
        // 刷新数据表
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getConfig());
        connector.setTable(connectorFactory.getTable(connectorInstance));
        profileComponent.editConfigModel(connector);
    }

    private MappingVo convertMapping2Vo(Mapping mapping) {
        String model = mapping.getModel();
        Assert.notNull(mapping, "Mapping can not be null.");
        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        ConnectorVo sConn = new ConnectorVo(connectorService.isAlive(s.getId()));
        BeanUtils.copyProperties(s, sConn);
        ConnectorVo tConn = new ConnectorVo(connectorService.isAlive(t.getId()));
        BeanUtils.copyProperties(t, tConn);

        // 元信息
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "Meta can not be null.");
        MetaVo metaVo = new MetaVo(ModelEnum.getModelEnum(model).getName(), mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);

        MappingVo vo = new MappingVo(sConn, tConn, metaVo);
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
        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        if (CollectionUtils.isEmpty(s.getTable()) || CollectionUtils.isEmpty(t.getTable())) {
            return;
        }

        // 匹配相似表
        for (Table sourceTable : s.getTable()) {
            if (StringUtil.isBlank(sourceTable.getName())) {
                continue;
            }
            for (Table targetTable : t.getTable()) {
                if (StringUtil.isBlank(targetTable.getName())) {
                    continue;
                }
                // 目标源表不支持视图
                if (TableTypeEnum.isView(targetTable.getType())) {
                    continue;
                }
                if (StringUtil.equalsIgnoreCase(sourceTable.getName(), targetTable.getName())) {
                    addTableGroup(mapping.getId(), sourceTable.getName(), targetTable.getName(), StringUtil.EMPTY);
                    break;
                }
            }
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
        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        if (CollectionUtils.isEmpty(s.getTable()) || CollectionUtils.isEmpty(t.getTable())) {
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
                        String name = replaceStar(m[0], tPk);;
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

}