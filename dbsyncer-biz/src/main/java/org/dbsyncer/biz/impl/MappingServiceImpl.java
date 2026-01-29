/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.*;
import org.dbsyncer.biz.checker.impl.mapping.MappingChecker;
import org.dbsyncer.biz.task.MappingCountTask;
import org.dbsyncer.biz.vo.ConnectorVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.dispatch.DispatchTaskService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.*;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.*;
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
    private LogService logService;

    @Override
    public String add(Map<String, String> params) throws Exception {
        ConfigModel model = mappingChecker.checkAddConfigModel(params);
        log(LogType.MappingLog.INSERT, model);

        // 先保存 Mapping，确保 Mapping 保存成功
        String id = profileComponent.addConfigModel(model);

        // Mapping 保存成功后，再创建并保存 Meta
        // 这样可以避免出现 Meta 存在但 Mapping 不存在的数据不一致问题
        Mapping mapping = (Mapping) model;
        Meta.create(mapping, snowflakeIdWorker, profileComponent);
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
        submitMappingCountTask((Mapping) model);
        return id;
    }

    @Override
    public String copy(String id) throws Exception {
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "The mapping id is invalid.");
        Mapping newMapping = mapping.copy(snowflakeIdWorker);
        log(LogType.MappingLog.COPY, newMapping);

        // 统计总数
        submitMappingCountTask(newMapping);
        return String.format("复制成功[%s]", newMapping.getName());
    }

    @Override
    public String edit(Map<String, String> params) throws Exception {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        String metaSnapshot = params.get("metaSnapshot");
        synchronized (LOCK) {
            // 检查是否禁止编辑
            mapping.assertDisableEdit();

            // 在保存之前，检查所有已存在的表映射关系的目标表是否存在
            checkAllTargetTablesExist(mapping);

            Mapping model = (Mapping) mappingChecker.checkEditConfigModel(params);
            log(LogType.MappingLog.UPDATE, model);

            // 更新meta
            mapping.updateMata(metaSnapshot);
        }
        submitMappingCountTask(mapping);
        return id;
    }

    /**
     * 检查所有已存在的表映射关系的目标表是否存在
     * 如果目标表不存在，抛出 TargetTableNotExistsException（包含所有缺失的表）
     * 注意：如果目标连接器不支持创建表（如 Kafka），则跳过检查
     *
     * @param mapping Mapping配置
     * @throws TargetTableNotExistsException 如果目标表不存在
     */
    private void checkAllTargetTablesExist(Mapping mapping) throws Exception {
        List<TableGroup> tableGroups = profileComponent.getTableGroupAll(mapping.getId());
        if (CollectionUtils.isEmpty(tableGroups)) {
            return;
        }

        // 检查目标连接器是否支持创建表
        Connector targetConnector = profileComponent.getConnector(mapping.getTargetConnectorId());
        org.dbsyncer.sdk.spi.ConnectorService<?, ?> targetConnectorService =
                connectorFactory.getConnectorService(targetConnector.getConfig().getConnectorType());

        // 如果目标连接器不支持创建表操作（如 Kafka），则跳过表存在性检查
        // 注意：这里使用 supportsCreateTable() 而不是 supportsDDLWrite()
        // - supportsCreateTable(): 判断是否支持"创建表"操作（如生成 CREATE TABLE DDL）
        // - supportsDDLWrite(): 判断是否支持"写入/执行"DDL 操作（如传递 DDL 消息）
        // 例如：Kafka 支持 writerDDL（传递 DDL 消息），但不支持 createTable（创建表）
        if (targetConnectorService != null && !targetConnectorService.supportsCreateTable()) {
            logger.debug("目标连接器 {} 不支持创建表操作，跳过表存在性检查", targetConnector.getConfig().getConnectorType());
            return;
        }

        // 收集所有缺失的表
        List<Map<String, String>> missingTables = new ArrayList<>();
        for (TableGroup tableGroup : tableGroups) {
            String targetTableName = tableGroup.getTargetTable().getName();
            // 获取目标表信息，如果表不存在，getMetaInfo 会返回空的 MetaInfo（字段列表为空）
            MetaInfo metaInfo = parserComponent.getMetaInfo(mapping.getTargetConnectorId(), targetTableName);
            // 判断表是否存在：MetaInfo 为空或字段列表为空表示表不存在
            if (metaInfo == null || CollectionUtils.isEmpty(metaInfo.getColumn())) {
                Map<String, String> tableMapping = new HashMap<>();
                tableMapping.put("sourceTable", tableGroup.getSourceTable().getName());
                tableMapping.put("targetTable", targetTableName);
                missingTables.add(tableMapping);
            }
        }

        // 如果有缺失的表，抛出异常（包含所有缺失的表信息）
        if (!missingTables.isEmpty()) {
            String message;
            if (missingTables.size() == 1) {
                message = "目标表不存在: " + missingTables.get(0).get("targetTable") + "，是否基于源表结构自动创建？";
            } else {
                message = "以下 " + missingTables.size() + " 个目标表不存在，是否基于源表结构自动创建？";
            }
            throw new TargetTableNotExistsException(message, missingTables);
        }
    }

    @Override
    public String remove(String id) throws Exception {
        Mapping mapping = assertMappingExist(id);
        String metaId = mapping.getMetaId();
        Meta meta = profileComponent.getMeta(metaId);
        synchronized (LOCK) {
            meta.assertRunning();

            // 删除数据
            monitorService.clearData(metaId);

            // 删除meta
            profileComponent.removeConfigModel(metaId);

            // 删除tableGroup
            List<TableGroup> groupList = profileComponent.getTableGroupAll(id);
            if (!CollectionUtils.isEmpty(groupList)) {
                groupList.forEach(t -> {
                    try {
                        profileComponent.removeTableGroup(t.getId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
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
        Assert.notNull(mapping, String.format("Mapping can not be null. id: %s", id));
        return convertMapping2Vo(mapping);
    }

    @Override
    public MappingVo getMapping(String id, Integer exclude) throws Exception {
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
            vo.getSourceConnector().setTable(vo.getSourceConnector().getTable().stream()
                    .filter(t -> !sTables.contains(t.getName())).collect(Collectors.toList()));
            vo.getTargetConnector().setTable(vo.getTargetConnector().getTable().stream()
                    .filter(t -> !tTables.contains(t.getName())).collect(Collectors.toList()));
            sTables.clear();
            tTables.clear();
        }
        return vo;
    }

    @Override
    public List<MappingVo> getMappingAll() {
        return profileComponent.getMappingAll()
                .stream()
                .map(mapping -> {
                    try {
                        return convertMapping2Vo(mapping);
                    } catch (Exception e) {
                        // 捕获其他可能的异常（如 Meta 为 null 等）
                        logger.error("转换 Mapping 失败，mappingId: {}, mappingName: {}",
                                mapping.getId(), mapping.getName(), e);
                        logService.log(LogType.MappingLog.CONFIG, String.format("mappingId: %s, msg: %s", mapping.getId(), e.getMessage()));
                        return createErrorMappingVo(mapping, e.getMessage());
                    }
                })
                .sorted(Comparator.comparing(MappingVo::getUpdateTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public String start(String id) throws Exception {
        Mapping mapping = assertMappingExist(id);

        synchronized (LOCK) {
            mapping.getMeta().assertRunning();

            // 启动
            managerFactory.start(mapping);

            log(LogType.MappingLog.RUNNING, mapping);
        }
        return "驱动启动成功";
    }

    @Override
    public String stop(String id) throws Exception {
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            if (!mapping.getMeta().isRunning()) {
                throw new BizException("驱动已停止.");
            }
            managerFactory.close(mapping);

            // 重置所有表组的同步速度为0
            List<TableGroup> tableGroupAll = profileComponent.getTableGroupAll(mapping.getId());
            for (TableGroup tableGroup : tableGroupAll) {
                tableGroup.setCurrentSpeed(0);
                profileComponent.editConfigModel(tableGroup);
            }

            log(LogType.MappingLog.STOP, mapping);

            // 发送关闭驱动通知消息
            String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
            sendNotifyMessage("停止驱动", String.format("手动停止驱动：%s(%s)", mapping.getName(), model));
        }
        return "驱动停止成功";
    }

    @Override
    public String refreshMappingTables(String id) throws Exception {
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "The mapping id is invalid.");

        // 检查是否禁止编辑
        mapping.assertDisableEdit();

        updateConnectorTables(mapping.getSourceConnectorId());
        updateConnectorTables(mapping.getTargetConnectorId());
        return "刷新驱动表成功";
    }

    /**
     * 提交统计驱动总数任务
     */
    private void submitMappingCountTask(Mapping mapping) {
        MappingCountTask task = new MappingCountTask();
        task.setMappingId(mapping.getId());
        task.setParserComponent(parserComponent);
        task.setProfileComponent(profileComponent);
        task.setTableGroupService(tableGroupService);
        dispatchTaskService.execute(task);
    }

    private void updateConnectorTables(String connectorId) throws Exception {
        Connector connector = profileComponent.getConnector(connectorId);
        Assert.notNull(connector, "The connector id is invalid.");
        // 刷新数据表
        ConnectorInstance connectorInstance = connectorFactory.connect(connector.getConfig());
        connector.setTable(connectorFactory.getTable(connectorInstance));
        profileComponent.editConfigModel(connector);
    }

    private MappingVo convertMapping2Vo(Mapping mapping) {
        String model = mapping.getModel();

        // 获取源连接器，如果为 null 则直接生成异常的 MappingVo
        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        if (s == null) {
            logger.error("源连接器不存在，mappingId: {}, sourceConnectorId: {}",
                    mapping.getId(), mapping.getSourceConnectorId());
            logService.log(LogType.MappingLog.CONFIG, String.format("源连接器不存在，mappingId: %s, sourceConnectorId: %s", mapping.getId(), mapping.getSourceConnectorId()));
            return createErrorMappingVo(mapping,
                    String.format("源连接器不存在，sourceConnectorId: %s", mapping.getSourceConnectorId()));
        }
        ConnectorVo sConn = new ConnectorVo(connectorService.isAlive(s.getId()));
        BeanUtils.copyProperties(s, sConn);

        // 获取目标连接器，如果为 null 则直接生成异常的 MappingVo
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        if (t == null) {
            logger.error("目标连接器不存在，mappingId: {}, targetConnectorId: {}",
                    mapping.getId(), mapping.getTargetConnectorId());
            logService.log(LogType.MappingLog.CONFIG, String.format("目标连接器不存在，mappingId: %s, targetConnectorId: %s", mapping.getId(), mapping.getSourceConnectorId()));
            return createErrorMappingVo(mapping,
                    String.format("目标连接器不存在，targetConnectorId: %s", mapping.getTargetConnectorId()));
        }
        ConnectorVo tConn = new ConnectorVo(connectorService.isAlive(t.getId()));
        BeanUtils.copyProperties(t, tConn);

        // 元信息
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "Meta can not be null.");
        MetaVo metaVo = new MetaVo(ModelEnum.getModelEnum(model).getName(), mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);
        metaVo.setCounting(dispatchTaskService.isRunning(mapping.getId()));

        MappingVo vo = new MappingVo(sConn, tConn, metaVo);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }

    /**
     * 创建标记为异常的 MappingVo（用于 getMappingAll 异常处理）
     *
     * @param mapping      原始 Mapping 对象
     * @param errorMessage 错误信息
     * @return 标记为异常的 MappingVo
     */
    private MappingVo createErrorMappingVo(Mapping mapping, String errorMessage) {
        // 创建默认的异常连接器
        ConnectorVo errorConn = new ConnectorVo(false);
        errorConn.setName("连接器异常");

        // 创建默认的 MetaVo
        MetaVo metaVo = new MetaVo("异常", mapping.getName());
        metaVo.setState(MetaEnum.ERROR.getCode());

        MappingVo vo = new MappingVo(errorConn, errorConn, metaVo);
        BeanUtils.copyProperties(mapping, vo);
        metaVo.setErrorMessage(errorMessage);
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
                addTableGroup(mapping.getId(), tableGroupNames[0], tableGroupNames[1],
                        tableGroup.length == 2 ? tableGroup[1] : StringUtil.EMPTY);
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
                        ;
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
        } catch (Exception e) {
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

    @Override
    public String reset(String id) throws Exception {
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "驱动不存在");

        synchronized (LOCK) {
            logger.info("重置驱动：{}", mapping.getName());
            Meta meta = profileComponent.getMeta(mapping.getMetaId());
            meta.clear(mapping.getModel());
            managerFactory.close(mapping);
            List<TableGroup> tableGroupAll = profileComponent.getTableGroupAll(mapping.getId());
            for (TableGroup tableGroup : tableGroupAll) {
                tableGroup.clear();
                profileComponent.editConfigModel(tableGroup);
            }

            mapping.setUpdateTime(Instant.now().toEpochMilli());
            profileComponent.editConfigModel(mapping);

            log(LogType.MappingLog.RESET, mapping);

            // 发送关闭驱动通知消息
            String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
            sendNotifyMessage("重置驱动", String.format("手动重置驱动：%s(%s)", mapping.getName(), model));
        }
        submitMappingCountTask(mapping);

        return "重置成功";
    }
}