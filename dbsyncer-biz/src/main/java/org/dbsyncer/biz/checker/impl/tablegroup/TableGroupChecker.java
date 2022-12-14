package org.dbsyncer.biz.checker.impl.tablegroup;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.enums.ModelEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.storage.constant.ConfigConstant;
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
        String sourceTablePK = params.get("sourceTablePK");
        String targetTablePK = params.get("targetTablePK");
        Assert.hasText(mappingId, "tableGroup mappingId is empty.");
        Assert.hasText(sourceTable, "tableGroup sourceTable is empty.");
        Assert.hasText(targetTable, "tableGroup targetTable is empty.");
        Mapping mapping = manager.getMapping(mappingId);
        Assert.notNull(mapping, "mapping can not be null.");

        // ????????????????????????????????????
        checkRepeatedTable(mappingId, sourceTable, targetTable);

        // ?????????????????????
        TableGroup tableGroup = new TableGroup();
        tableGroup.setFieldMapping(new ArrayList<>());
        tableGroup.setMappingId(mappingId);
        tableGroup.setSourceTable(getTable(mapping.getSourceConnectorId(), sourceTable, sourceTablePK));
        tableGroup.setTargetTable(getTable(mapping.getTargetConnectorId(), targetTable, targetTablePK));
        tableGroup.setParams(new HashMap<>());

        // ??????????????????
        this.modifyConfigModel(tableGroup, params);

        // ??????????????????????????????
        mergeFieldMapping(tableGroup);

        // ????????????
        mergeConfig(mapping, tableGroup);

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

        // ??????????????????
        this.modifyConfigModel(tableGroup, params);

        // ?????????????????????????????????/????????????/????????????
        this.modifySuperConfigModel(tableGroup, params);

        // ??????????????????
        setFieldMapping(tableGroup, fieldMappingJson);

        // ????????????
        mergeConfig(mapping, tableGroup);

        return tableGroup;
    }

    public void mergeConfig(Mapping mapping, TableGroup tableGroup) {
        // ??????????????????
        TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);

        // ????????????
        dealIncrementStrategy(mapping, group);

        Map<String, String> command = manager.getCommand(mapping, group);
        tableGroup.setCommand(command);

        // ?????????????????????
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

    private Table getTable(String connectorId, String tableName, String primaryKey) {
        MetaInfo metaInfo = manager.getMetaInfo(connectorId, tableName);
        Assert.notNull(metaInfo, "??????????????????????????????.");
        // ???????????????
        if(StringUtil.isNotBlank(primaryKey) && !CollectionUtils.isEmpty(metaInfo.getColumn())){
            for(Field field : metaInfo.getColumn()){
                if(StringUtil.equals(field.getName(), primaryKey)){
                    field.setPk(true);
                    break;
                }
            }
        }
        return new Table(tableName, metaInfo.getTableType(), primaryKey, metaInfo.getColumn());
    }

    private void checkRepeatedTable(String mappingId, String sourceTable, String targetTable) {
        List<TableGroup> list = manager.getTableGroupAll(mappingId);
        if (!CollectionUtils.isEmpty(list)) {
            for (TableGroup g : list) {
                // ?????????????????????????????????
                if (StringUtil.equals(sourceTable, g.getSourceTable().getName())
                        && StringUtil.equals(targetTable, g.getTargetTable().getName())) {
                    final String error = String.format("?????????????????????.%s > %s", sourceTable, targetTable);
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

        // Set????????????
        Map<String, Field> m1 = new HashMap<>();
        Map<String, Field> m2 = new HashMap<>();
        List<String> k1 = new LinkedList<>();
        List<String> k2 = new LinkedList<>();
        shuffleColumn(sCol, k1, m1);
        shuffleColumn(tCol, k2, m2);
        k1.retainAll(k2);

        // ???????????????
        if (!CollectionUtils.isEmpty(k1)) {
            k1.forEach(k -> tableGroup.getFieldMapping().add(new FieldMapping(m1.get(k), m2.get(k))));
        }
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
     * ??????????????????
     *
     * @param tableGroup
     * @param json       [{"source":"id","target":"id"}]
     * @return
     */
    private void setFieldMapping(TableGroup tableGroup, String json) {
        JSONArray mapping = JsonUtil.parseArray(json);
        if (null == mapping) {
            throw new BizException("????????????????????????");
        }

        final Map<String, Field> sMap = PickerUtil.convert2Map(tableGroup.getSourceTable().getColumn());
        final Map<String, Field> tMap = PickerUtil.convert2Map(tableGroup.getTargetTable().getColumn());
        int length = mapping.size();
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
    }

}