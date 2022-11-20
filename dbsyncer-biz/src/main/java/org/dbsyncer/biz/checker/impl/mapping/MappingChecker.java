package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker;
import org.dbsyncer.common.util.*;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class MappingChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private TableGroupChecker tableGroupChecker;

    @Autowired
    private Map<String, MappingConfigChecker> map;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String sourceConnectorId = params.get("sourceConnectorId");
        String targetConnectorId = params.get("targetConnectorId");
        Assert.hasText(name, "驱动名称不能为空");
        Assert.hasText(sourceConnectorId, "数据源不能为空.");
        Assert.hasText(targetConnectorId, "目标源不能为空.");

        Mapping mapping = new Mapping();
        mapping.setName(name);
        mapping.setType(ConfigConstant.MAPPING);
        mapping.setSourceConnectorId(sourceConnectorId);
        mapping.setTargetConnectorId(targetConnectorId);
        mapping.setModel(ModelEnum.FULL.getCode());
        mapping.setListener(new ListenerConfig(ListenerTypeEnum.LOG.getType()));
        mapping.setParams(new HashMap<>());

        // 修改基本配置
        this.modifyConfigModel(mapping, params);

        // 创建meta
        addMeta(mapping);

        return mapping;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "MappingChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = manager.getMapping(id);
        Assert.notNull(mapping, "Can not find mapping.");

        // 修改基本配置
        this.modifyConfigModel(mapping, params);

        // 同步方式(仅支持全量或增量同步方式)
        String model = params.get("model");
        mapping.setModel(null != ModelEnum.getModelEnum(model) ? model : ModelEnum.FULL.getCode());

        // 全量配置
        mapping.setReadNum(NumberUtil.toInt(params.get("readNum"), mapping.getReadNum()));
        mapping.setBatchNum(NumberUtil.toInt(params.get("batchNum"), mapping.getBatchNum()));
        mapping.setThreadNum(NumberUtil.toInt(params.get("threadNum"), mapping.getThreadNum()));

        // 增量配置(日志/定时)
        String incrementStrategy = params.get("incrementStrategy");
        Assert.hasText(incrementStrategy, "MappingChecker check params incrementStrategy is empty");
        String type = StringUtil.toLowerCaseFirstOne(incrementStrategy).concat("ConfigChecker");
        MappingConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(mapping, params);

        // 自定义监听事件配置
        updateListenerConfig(mapping.getListener(), params);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(mapping, params);

        // 更新meta
        String metaSnapshot = params.get("metaSnapshot");
        updateMeta(mapping, metaSnapshot);

        return mapping;
    }

    /**
     * 更新元信息
     *
     * @param mapping
     */
    public void updateMeta(Mapping mapping) {
        updateMeta(mapping, null);
    }

    /**
     * 合并关联的映射关系配置
     *
     * @param mapping
     * @param params
     */
    public void batchMergeTableGroupConfig(Mapping mapping, Map<String, String> params) {
        List<TableGroup> groupAll = manager.getTableGroupAll(mapping.getId());
        if (!CollectionUtils.isEmpty(groupAll)) {
            // 手动排序
            String[] sortedTableGroupIds = StringUtil.split(params.get("sortedTableGroupIds"), "|");
            if (null != sortedTableGroupIds && sortedTableGroupIds.length > 0) {
                Map<String, TableGroup> tableGroupMap = groupAll.stream().collect(Collectors.toMap(TableGroup::getId, f -> f, (k1, k2) -> k1));
                groupAll.clear();
                int size = sortedTableGroupIds.length;
                int i = size;
                while (i > 0){
                    TableGroup g = tableGroupMap.get(sortedTableGroupIds[size - i]);
                    Assert.notNull(g, "Invalid sorted tableGroup.");
                    g.setIndex(i);
                    groupAll.add(g);
                    i--;
                }
            }

            // 合并配置
            for (TableGroup g : groupAll) {
                tableGroupChecker.mergeConfig(mapping, g);
                manager.editConfigModel(g);
            }
        }
    }

    /**
     * 修改监听器配置
     *
     * @param listener
     * @param params
     */
    private void updateListenerConfig(ListenerConfig listener, Map<String, String> params) {
        Assert.notNull(listener, "ListenerConfig can not be null.");
        String banUpdate = StringUtil.isNotBlank(params.get("banUpdate")) ? "true" : "false";
        String banInsert = StringUtil.isNotBlank(params.get("banInsert")) ? "true" : "false";
        String banDelete = StringUtil.isNotBlank(params.get("banDelete")) ? "true" : "false";

        listener.setBanUpdate(BooleanUtil.toBoolean(banUpdate));
        listener.setBanInsert(BooleanUtil.toBoolean(banInsert));
        listener.setBanDelete(BooleanUtil.toBoolean(banDelete));
    }

    /**
     * 更新元信息
     *
     * @param mapping
     * @param metaSnapshot
     */
    private void updateMeta(Mapping mapping, String metaSnapshot) {
        Meta meta = manager.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "驱动meta不存在.");

        // 清空状态
        meta.clear();

        // 手动配置增量点
        if(StringUtil.isNotBlank(metaSnapshot)){
            Map snapshot = JsonUtil.jsonToObj(metaSnapshot, HashMap.class);
            if(!CollectionUtils.isEmpty(snapshot)){
                meta.setSnapshot(snapshot);
            }
        }

        getMetaTotal(meta, mapping.getModel());

        meta.setUpdateTime(Instant.now().toEpochMilli());
        manager.editConfigModel(meta);
    }

    private void addMeta(Mapping mapping) {
        Meta meta = new Meta();
        meta.setMappingId(mapping.getId());
        meta.setType(ConfigConstant.META);
        meta.setName(ConfigConstant.META);

        // 修改基本配置
        this.modifyConfigModel(meta, new HashMap<>());

        String id = manager.addConfigModel(meta);
        mapping.setMetaId(id);
    }

    private void getMetaTotal(Meta meta, String model) {
        // 全量同步
        if (ModelEnum.isFull(model)) {
            // 统计tableGroup总条数
            AtomicLong count = new AtomicLong(0);
            List<TableGroup> groupAll = manager.getTableGroupAll(meta.getMappingId());
            if (!CollectionUtils.isEmpty(groupAll)) {
                for (TableGroup g : groupAll) {
                    count.getAndAdd(g.getSourceTable().getCount());
                }
            }
            meta.setTotal(count);
        }
    }

}