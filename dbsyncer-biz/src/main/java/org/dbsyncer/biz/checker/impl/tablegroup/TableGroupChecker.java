package org.dbsyncer.biz.checker.impl.tablegroup;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.MetaInfo;
import org.dbsyncer.connector.config.Table;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class TableGroupChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private Map<String, ConnectorConfigChecker> map;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        String mappingId = params.get("mappingId");
        String sourceTable = params.get("sourceTable");
        String targetTable = params.get("targetTable");
        Assert.hasText(mappingId, "tableGroup mappingId is empty.");
        Assert.hasText(sourceTable, "tableGroup sourceTable is empty.");
        Assert.hasText(targetTable, "tableGroup targetTable is empty.");
        Mapping mapping = manager.getMapping(mappingId);
        Assert.notNull(mapping, "mapping can not be null.");

        // 检查是否存在重复映射关系
        checkRepeatedTable(mappingId, sourceTable, targetTable);

        // 获取连接器信息
        TableGroup tableGroup = new TableGroup();
        tableGroup.setFieldMapping(new ArrayList<>());
        tableGroup.setName(ConfigConstant.TABLE_GROUP);
        tableGroup.setType(ConfigConstant.TABLE_GROUP);
        tableGroup.setMappingId(mappingId);
        tableGroup.setSourceTable(getTable(mapping.getSourceConnectorId(), sourceTable));
        tableGroup.setTargetTable(getTable(mapping.getTargetConnectorId(), targetTable));

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 处理策略
        dealIncrementStrategy(mapping, tableGroup);

        // 匹配相似字段
        mergeFieldMapping(tableGroup);

        // 生成command
        genCommand(mapping, tableGroup);

        return tableGroup;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "TableGroupChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = manager.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        Mapping mapping = manager.getMapping(tableGroup.getMappingId());
        Assert.notNull(mapping, "mapping can not be null.");
        String fieldMappingJson = params.get("fieldMapping");
        Assert.hasText(fieldMappingJson, "TableGroupChecker check params fieldMapping is empty");

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 字段映射关系
        setFieldMapping(tableGroup, fieldMappingJson);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(tableGroup, params);

        // 生成command
        genCommand(mapping, tableGroup);

        return tableGroup;
    }

    public void genCommand(Mapping mapping, TableGroup tableGroup) {
        TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);

        Map<String, String> command = manager.getCommand(mapping, group);
        tableGroup.setCommand(command);

        // 获取数据源总数
        long count = ModelEnum.isFull(mapping.getModel()) && !CollectionUtils.isEmpty(command) ? manager.getCount(mapping.getSourceConnectorId(), command) : 0;
        tableGroup.getSourceTable().setCount(count);
    }

    public void dealIncrementStrategy(Mapping mapping, TableGroup tableGroup) {
        String connectorType = manager.getConnector(mapping.getSourceConnectorId()).getConfig().getConnectorType();
        String type = StringUtil.toLowerCaseFirstOne(connectorType).concat("ConfigChecker");
        ConnectorConfigChecker checker = map.get(type);
        Assert.notNull(checker, "Checker can not be null.");
        checker.dealIncrementStrategy(mapping, tableGroup);
    }

    private Table getTable(String connectorId, String tableName) {
        MetaInfo metaInfo = manager.getMetaInfo(connectorId, tableName);
        Assert.notNull(metaInfo, "无法获取连接器表信息.");
        return new Table().setName(tableName).setColumn(metaInfo.getColumn());
    }

    private void checkRepeatedTable(String mappingId, String sourceTable, String targetTable) {
        List<TableGroup> list = manager.getTableGroupAll(mappingId);
        if (!CollectionUtils.isEmpty(list)) {
            for (TableGroup g : list) {
                // 数据源表和目标表都存在
                if (StringUtils.equals(sourceTable, g.getSourceTable().getName())
                        && StringUtils.equals(targetTable, g.getTargetTable().getName())) {
                    final String error = String.format("映射关系已存在.%s > %s", sourceTable, targetTable);
                    logger.error(error);
                    throw new BizException(error);
                }
            }
        }
    }

    private void mergeFieldMapping(TableGroup tableGroup) {
        List<Field> sCol = tableGroup.getSourceTable().getColumn();
        List<Field> tCol = tableGroup.getTargetTable().getColumn();
        if (CollectionUtils.isEmpty(sCol) || CollectionUtils.isEmpty(tCol)) {
            return;
        }

        // Set集合去重
        Map<String, Field> m1 = new HashMap<>();
        Map<String, Field> m2 = new HashMap<>();
        List<String> k1 = new LinkedList<>();
        List<String> k2 = new LinkedList<>();
        shuffleColumn(sCol, k1, m1);
        shuffleColumn(tCol, k2, m2);
        k1.retainAll(k2);

        // 有相似字段
        List<FieldMapping> fields = new ArrayList<>();
        if (!CollectionUtils.isEmpty(k1)) {
            k1.forEach(k -> fields.add(new FieldMapping(m1.get(k), m2.get(k))));
        }
        tableGroup.getFieldMapping().addAll(fields);
    }

    private void shuffleColumn(List<Field> col, List<String> key, Map<String, Field> map) {
        col.forEach(f -> {
            if (!key.contains(f.getName())) {
                key.add(f.getName());
                map.put(f.getName(), f);
            }
        });
    }

    /**
     * 解析映射关系
     *
     * @param tableGroup
     * @param json       [{"source":"id","target":"id"}]
     * @return
     */
    private void setFieldMapping(TableGroup tableGroup, String json) {
        try {
            JSONArray mapping = new JSONArray(json);
            if (null == mapping) {
                throw new BizException("映射关系不能为空");
            }

            final Map<String, Field> sMap = PickerUtil.convert2Map(tableGroup.getSourceTable().getColumn());
            final Map<String, Field> tMap = PickerUtil.convert2Map(tableGroup.getTargetTable().getColumn());
            int length = mapping.length();
            List<FieldMapping> list = new ArrayList<>();
            JSONObject row = null;
            Field s = null;
            Field t = null;
            for (int i = 0; i < length; i++) {
                row = mapping.getJSONObject(i);
                s = sMap.get(row.getString("source"));
                t = tMap.get(row.getString("target"));
                if (null == s && null == t) {
                    continue;
                }

                if (null != t) {
                    t.setPk(row.getBoolean("pk"));
                }
                list.add(new FieldMapping(s, t));
            }
            tableGroup.setFieldMapping(list);
        } catch (JSONException e) {
            logger.error(e.getMessage());
            throw new BizException(e.getMessage());
        }
    }

}