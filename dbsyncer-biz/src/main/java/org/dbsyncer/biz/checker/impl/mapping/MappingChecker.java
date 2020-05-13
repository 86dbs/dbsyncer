package org.dbsyncer.biz.checker.impl.mapping;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.enums.IncrementEnum;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.*;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class MappingChecker extends AbstractChecker implements ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private TableGroupChecker tableGroupChecker;

    private Map<String, MappingConfigChecker> map;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        map = applicationContext.getBeansOfType(MappingConfigChecker.class);
    }

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
        mapping.setListener(new ListenerConfig(IncrementEnum.TIMING.getType()));

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
        if (StringUtils.isNotBlank(model)) {
            if (null != ModelEnum.getModelEnum(model)) {
                mapping.setModel(model);
            }
        }

        // 全量配置
        String readNum = params.get("readNum");
        mapping.setReadNum(NumberUtils.toInt(readNum, mapping.getReadNum()));
        String threadNum = params.get("threadNum");
        mapping.setThreadNum(NumberUtils.toInt(threadNum, mapping.getThreadNum()));
        String batchNum = params.get("batchNum");
        mapping.setBatchNum(NumberUtils.toInt(batchNum, mapping.getBatchNum()));

        // 增量配置(日志/定时)
        String incrementStrategy = params.get("incrementStrategy");
        Assert.hasText(incrementStrategy, "MappingChecker check params incrementStrategy is empty");
        String type = StringUtil.toLowerCaseFirstOne(incrementStrategy).concat("ConfigChecker");
        MappingConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.modify(mapping, params);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(mapping, params);

        // 更新映射关系过滤条件
        setFilterCommand(mapping);

        // 更新meta
        updateMeta(mapping);

        return mapping;
    }

    /**
     * <b>更新映射关系过滤条件</b>
     * <p>如果映射关系没有过滤条件，使用全局的过滤条件</p>
     *
     * @param mapping
     */
    private void setFilterCommand(Mapping mapping) {
        List<TableGroup> groupAll = manager.getTableGroupAll(mapping.getId());
        if (!CollectionUtils.isEmpty(groupAll)) {
            for (TableGroup g : groupAll) {
                tableGroupChecker.setCommand(mapping, g);
                manager.editTableGroup(g);
            }
        }
    }

    private void addMeta(Mapping mapping) {
        Meta meta = new Meta();
        meta.setMappingId(mapping.getId());
        meta.setType(ConfigConstant.META);
        meta.setName(ConfigConstant.META);

        // 修改基本配置
        this.modifyConfigModel(meta, new HashMap<>());

        String id = manager.addMeta(meta);
        mapping.setMetaId(id);
    }

    private void updateMeta(Mapping mapping) {
        Meta meta = manager.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "驱动meta不存在.");

        // 清空状态
        meta.clear();

        getMetaTotal(meta, mapping.getModel());

        manager.editMeta(meta);
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