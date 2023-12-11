package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.checker.impl.mapping.MappingChecker;
import org.dbsyncer.biz.vo.ConnectorVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.MetaVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class MappingServiceImpl extends BaseServiceImpl implements MappingService {

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
    private ManagerFactory managerFactory;

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = mappingChecker.checkAddConfigModel(params);
        log(LogType.MappingLog.INSERT, (Mapping) model);

        String id = profileComponent.addConfigModel(model);

        // 匹配相似表 on
        String autoMatchTable = params.get("autoMatchTable");
        if (StringUtil.isNotBlank(autoMatchTable)) {
            matchSimilarTable(model);
        }

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

            mappingChecker.batchMergeTableGroupConfig(model, params);
            return profileComponent.editConfigModel(model);
        }
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
    public List<MappingVo> getMappingAll() {
        List<MappingVo> list = profileComponent.getMappingAll()
                .stream()
                .map(m -> convertMapping2Vo(m))
                .sorted(Comparator.comparing(MappingVo::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return list;
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

        // 按升序展示表
        Collections.sort(sConn.getTable(), Comparator.comparing(Table::getName));
        Collections.sort(tConn.getTable(), Comparator.comparing(Table::getName));

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
    private void matchSimilarTable(ConfigModel model) {
        Mapping mapping = (Mapping) model;
        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        if (CollectionUtils.isEmpty(s.getTable()) || CollectionUtils.isEmpty(t.getTable())) {
            return;
        }

        // 存在交集
        List<String> sTables = s.getTable().stream().map(table -> table.getName()).collect(Collectors.toList());
        List<String> tTables = t.getTable().stream().map(table -> table.getName()).collect(Collectors.toList());
        sTables.retainAll(tTables);
        if (!CollectionUtils.isEmpty(sTables)) {
            Map<String, String> params = new HashMap<>();
            params.put("mappingId", mapping.getId());
            sTables.forEach(table -> {
                params.put("sourceTable", table);
                params.put("targetTable", table);
                tableGroupService.add(params);
            });
            mappingChecker.updateMeta(mapping);
        }
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