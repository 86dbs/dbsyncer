/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private ParserComponent parserComponent;

    @Resource
    private Map<String, MappingConfigChecker> map;

    @Resource
    ConnectorFactory connectorFactory;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) throws Exception {
        logger.info("params:{}", params);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String sourceConnectorId = params.get("sourceConnectorId");
        String targetConnectorId = params.get("targetConnectorId");
        Assert.hasText(name, "驱动名称不能为空");
        Assert.hasText(sourceConnectorId, "数据源不能为空.");
        Assert.hasText(targetConnectorId, "目标源不能为空.");

        // 验证连接器是否存在
        validateConnectorExists(sourceConnectorId, "源连接器");
        validateConnectorExists(targetConnectorId, "目标连接器");

        Mapping mapping = new Mapping();
        mapping.profileComponent = profileComponent;
        mapping.setName(name);
        mapping.setSourceConnectorId(sourceConnectorId);
        mapping.setTargetConnectorId(targetConnectorId);
        mapping.setListener(new ListenerConfig(ListenerTypeEnum.LOG.getType()));
        mapping.setParams(new HashMap<>());

        // 修改基本配置
        this.modifyConfigModel(mapping, params);

        // 注意：不再在此处创建 Meta，改为在 MappingServiceImpl.add() 中保存 Mapping 成功后再创建
        // 这样可以确保先保存 Mapping，避免出现 Meta 存在但 Mapping 不存在的数据不一致问题

        // 同步方式
        String model = params.get("model");
        mapping.setModel(ModelEnum.getModelEnum(model).getCode());
        return mapping;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) throws Exception {
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
        batchMergeConfig(mapping, params);

        return mapping;
    }

    /**
     * 修改监听器配置
     *
     * @param listener
     * @param params
     */
    private void updateListenerConfig(ListenerConfig listener, Map<String, String> params) {
        Assert.notNull(listener, "ListenerConfig can not be null.");

        // 现有配置
        listener.setEnableUpdate(StringUtil.isNotBlank(params.get("enableUpdate")));
        listener.setEnableInsert(StringUtil.isNotBlank(params.get("enableInsert")));
        listener.setEnableDelete(StringUtil.isNotBlank(params.get("enableDelete")));
        listener.setEnableDDL(StringUtil.isNotBlank(params.get("enableDDL")));

        // 新增 DDL 细粒度配置
        // 注意：使用 StringUtil.isNotBlank 判断，空字符串视为 false
        // 如果参数不存在，使用缺省值（已在 ListenerConfig 中设置）
        if (params.containsKey("allowAddColumn")) {
            listener.setAllowAddColumn(StringUtil.isNotBlank(params.get("allowAddColumn")));
        }
        if (params.containsKey("allowDropColumn")) {
            listener.setAllowDropColumn(StringUtil.isNotBlank(params.get("allowDropColumn")));
        }
        if (params.containsKey("allowModifyColumn")) {
            listener.setAllowModifyColumn(StringUtil.isNotBlank(params.get("allowModifyColumn")));
        }
    }

    private void batchMergeConfig(Mapping mapping, Map<String, String> params) throws Exception {
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(mapping.getId());
        if (!CollectionUtils.isEmpty(groupAll)) {
            // 手动排序
            String[] sortedTableGroupIds = StringUtil.split(params.get("sortedTableGroupIds"),
                    StringUtil.VERTICAL_LINE);
            if (null != sortedTableGroupIds && sortedTableGroupIds.length > 0) {
                Map<String, TableGroup> tableGroupMap = groupAll.stream()
                        .collect(Collectors.toMap(TableGroup::getId, f -> f, (k1, k2) -> k1));
                groupAll.clear();
                int size = sortedTableGroupIds.length;
                int i = size;
                while (i > 0) {
                    TableGroup g = tableGroupMap.get(sortedTableGroupIds[size - i]);
                    Assert.notNull(g, "Invalid sorted tableGroup.");
                    g.setIndex(i);
                    groupAll.add(g);
                    i--;
                }
            }

            // 更新 sql 配置
            for (TableGroup g : groupAll) {
                // 初始化 TableGroup 的运行时组件
                g.initTableGroup(parserComponent, profileComponent, connectorFactory);
                profileComponent.editConfigModel(g);
            }
        }
    }

    /**
     * 验证连接器是否存在
     *
     * @param connectorId 连接器ID
     * @param connectorType 连接器类型描述（用于错误提示）
     * @throws org.dbsyncer.parser.ParserException 如果连接器不存在
     */
    private void validateConnectorExists(String connectorId, String connectorType) {
        Connector connector = profileComponent.getConnector(connectorId);
        if (connector == null) {
            throw new org.dbsyncer.parser.ParserException(
                    String.format("%s不存在，connectorId: %s", connectorType, connectorId));
        }
    }
}