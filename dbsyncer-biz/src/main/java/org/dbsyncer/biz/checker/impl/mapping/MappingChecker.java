/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.biz.checker.impl.tablegroup.TableGroupChecker;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class MappingChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private TableGroupChecker tableGroupChecker;

    @Resource
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
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "Can not find mapping.");

        // 修改基本配置
        this.modifyConfigModel(mapping, params);

        // 同步方式(仅支持全量或增量同步方式)
        String model = params.get("model");
        mapping.setModel(ModelEnum.getModelEnum(model).getCode());

        // 全量配置
        mapping.setReadNum(NumberUtil.toInt(params.get("readNum"), mapping.getReadNum()));
        mapping.setBatchNum(NumberUtil.toInt(params.get("batchNum"), mapping.getBatchNum()));
        mapping.setThreadNum(NumberUtil.toInt(params.get("threadNum"), mapping.getThreadNum()));
        mapping.setForceUpdate(StringUtil.isNotBlank(params.get("forceUpdate")));

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

        // 合并关联的映射关系配置
        tableGroupChecker.batchMergeConfig(mapping, params);

        return mapping;
    }

    public void addMeta(Mapping mapping) {
        Meta meta = new Meta();
        meta.setMappingId(mapping.getId());

        // 修改基本配置
        this.modifyConfigModel(meta, new HashMap<>());

        String id = profileComponent.addConfigModel(meta);
        mapping.setMetaId(id);
    }

    /**
     * 修改监听器配置
     *
     * @param listener
     * @param params
     */
    private void updateListenerConfig(ListenerConfig listener, Map<String, String> params) {
        Assert.notNull(listener, "ListenerConfig can not be null.");

        listener.setEnableUpdate(StringUtil.isNotBlank(params.get("enableUpdate")));
        listener.setEnableInsert(StringUtil.isNotBlank(params.get("enableInsert")));
        listener.setEnableDelete(StringUtil.isNotBlank(params.get("enableDelete")));
        listener.setEnableDDL(StringUtil.isNotBlank(params.get("enableDDL")));
    }

}