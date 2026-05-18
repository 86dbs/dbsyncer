/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.tablegroup;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.RepeatedTableGroupException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.sdk.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
@Component
public class TableGroupChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        String mappingId = params.get("mappingId");
        String sourceTable = params.get("sourceTable");
        String targetTable = params.get("targetTable");
        String sourceType = params.get("sourceType");
        String targetType = params.get("targetType");
        String sourceTablePK = params.get("sourceTablePK");
        String targetTablePK = params.get("targetTablePK");
        String fieldMappings = params.get("fieldMappings");
        Assert.hasText(mappingId, "tableGroup mappingId is empty.");
        Assert.hasText(sourceTable, "tableGroup sourceTable is empty.");
        Assert.hasText(targetTable, "tableGroup targetTable is empty.");
        Assert.hasText(sourceType, "tableGroup sourceType is empty.");
        Assert.hasText(targetType, "tableGroup targetType is empty.");
        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "mapping can not be null.");

        // 检查是否存在重复映射关系
        checkRepeatedTable(profileComponent.getTableGroupAll(mappingId), sourceTable, targetTable);

        // 获取连接器信息
        TableGroup tableGroup = new TableGroup();
        tableGroup.setMappingId(mappingId);
        Table source = findTable(mapping.getSourceTable(), sourceTable, sourceType);
        Table target = findTable(mapping.getTargetTable(), targetTable, targetType);
        tableGroup.setSourceTable(updateTableColumn(mapping, ConnectorInstanceUtil.SOURCE_SUFFIX, sourceTablePK, source));
        tableGroup.setTargetTable(updateTableColumn(mapping, ConnectorInstanceUtil.TARGET_SUFFIX, targetTablePK, target));

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 匹配相似字段映射关系
        if (StringUtil.isNotBlank(fieldMappings)) {
            matchFieldMapping(tableGroup, fieldMappings);
        } else {
            matchFieldMapping(tableGroup);
        }

        // 合并配置
        mergeConfig(mapping, tableGroup);

        return tableGroup;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "TableGroupChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
        Assert.notNull(mapping, "mapping can not be null.");
        String fieldMappingJson = params.get("fieldMapping");
        Assert.hasText(fieldMappingJson, "TableGroupChecker check params fieldMapping is empty");

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(tableGroup, params);

        // 字段映射关系
        setFieldMapping(tableGroup, fieldMappingJson);

        // 合并配置
        mergeConfig(mapping, tableGroup);

        return tableGroup;
    }

    public Table findTable(List<Table> tables, String tableName, String type) {
        if (!CollectionUtils.isEmpty(tables)) {
            Optional<Table> first = tables.stream().filter(table -> table.getName().equals(tableName) && table.getType().equals(type)).findFirst();
            if (first.isPresent()) {
                return first.get();
            }
        }
        throw new BizException("TableName not found.");
    }

    /**
     * 刷新表字段
     */
    public void refreshTableFields(TableGroup tableGroup) {
        Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
        Assert.notNull(mapping, "mapping can not be null.");

        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();
        List<String> sourceTablePks = sourceTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        List<String> targetTablePks = targetTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        updateTableColumn(mapping, ConnectorInstanceUtil.SOURCE_SUFFIX, StringUtil.join(sourceTablePks, ","), sourceTable);
        updateTableColumn(mapping, ConnectorInstanceUtil.TARGET_SUFFIX, StringUtil.join(targetTablePks, ","), targetTable);
    }

    public void mergeConfig(Mapping mapping, TableGroup tableGroup) {
        // 合并高级配置
        TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        Map<String, String> command = parserComponent.getCommand(mapping, group);
        tableGroup.setCommand(command);
    }

    /**
     * 订正校验任务：合并任务级过滤等与表组配置后
     *
     * @param task       订正校验任务
     * @param tableGroup 已持久化的表组
     */
    public void mergeConfig(ValidateSyncTask task, TableGroup tableGroup) {
        TableGroup group = PickerUtil.mergeTableGroupConfig(task, tableGroup);
        Map<String, String> command = parserComponent.getCommand(task, group);
        tableGroup.setCommand(command);
    }

    private Table updateTableColumn(Mapping mapping, String suffix, String primaryKeyStr, Table table) {
        boolean isSource = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(mapping, isSource);
        updateTableColumn(context, primaryKeyStr, table);
        return table;
    }

    public Table updateTableColumn(DefaultConnectorServiceContext context, String primaryKeyStr, Table table) {
        context.addTablePattern(table);
        List<MetaInfo> metaInfos = parserComponent.getMetaInfo(context);
        MetaInfo metaInfo = CollectionUtils.isEmpty(metaInfos) ? null : metaInfos.get(0);
        Assert.notNull(metaInfo, "无法获取连接器表信息");
        // 自定义主键
        if (StringUtil.isNotBlank(primaryKeyStr) && !CollectionUtils.isEmpty(metaInfo.getColumn())) {
            String[] pks = StringUtil.split(primaryKeyStr, StringUtil.COMMA);
            Arrays.stream(pks).forEach(pk -> {
                for (Field field : metaInfo.getColumn()) {
                    if (StringUtil.equalsIgnoreCase(field.getName(), pk)) {
                        field.setPk(true);
                        break;
                    }
                }
            });
        }
        table.setColumn(metaInfo.getColumn());
        return table;
    }

    public void checkRepeatedTable(List<TableGroup> list, String sourceTable, String targetTable) {
        if (!CollectionUtils.isEmpty(list)) {
            for (TableGroup g : list) {
                // 数据源表和目标表都存在
                if (StringUtil.equals(sourceTable, g.getSourceTable().getName()) && StringUtil.equals(targetTable, g.getTargetTable().getName())) {
                    final String error = String.format("映射关系已存在.%s > %s", sourceTable, targetTable);
                    logger.error(error);
                    throw new RepeatedTableGroupException(error);
                }
            }
        }
    }

    public void matchFieldMapping(TableGroup tableGroup) {
        List<Field> sCol = tableGroup.getSourceTable().getColumn();
        List<Field> tCol = tableGroup.getTargetTable().getColumn();
        if (CollectionUtils.isEmpty(sCol) || CollectionUtils.isEmpty(tCol)) {
            return;
        }

        Map<String, Field> m1 = new LinkedHashMap<>();
        Map<String, Field> m2 = new LinkedHashMap<>();
        shuffleColumn(sCol, m1);
        shuffleColumn(tCol, m2);

        // 模糊匹配相似字段
        AtomicBoolean existSourcePKFieldMapping = new AtomicBoolean();
        AtomicBoolean existTargetPKFieldMapping = new AtomicBoolean();
        m1.forEach((s, f1) -> m2.computeIfPresent(s, (t, f2) -> {
            tableGroup.getFieldMapping().add(new FieldMapping(f1, f2));
            if (f1.isPk()) {
                existSourcePKFieldMapping.set(true);
            }
            if (f2.isPk()) {
                existTargetPKFieldMapping.set(true);
            }
            return f2;
        }));

        // 沒有主键映射关系，取第一个主键作为映射关系
        if (!existSourcePKFieldMapping.get() || !existTargetPKFieldMapping.get()) {
            List<String> sourceTablePrimaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(tableGroup.getSourceTable());
            List<String> targetTablePrimaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(tableGroup.getTargetTable());
            Assert.isTrue(!CollectionUtils.isEmpty(sourceTablePrimaryKeys) && !CollectionUtils.isEmpty(targetTablePrimaryKeys), "数据源表和目标源表必须包含主键.");
            String sPK = sourceTablePrimaryKeys.stream().findFirst().get().toUpperCase();
            String tPK = targetTablePrimaryKeys.stream().findFirst().get().toUpperCase();
            tableGroup.getFieldMapping().add(new FieldMapping(m1.get(sPK), m2.get(tPK)));
        }
    }

    public void matchFieldMapping(TableGroup tableGroup, String fieldMappings) {
        // A1|A2,B1|B2,|C2
        List<Field> sCol = tableGroup.getSourceTable().getColumn();
        List<Field> tCol = tableGroup.getTargetTable().getColumn();
        if (CollectionUtils.isEmpty(sCol) || CollectionUtils.isEmpty(tCol) || StringUtil.isBlank(fieldMappings)) {
            return;
        }

        Map<String, Field> sMap = sCol.stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> tMap = tCol.stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        List<FieldMapping> fieldMappingList = tableGroup.getFieldMapping();
        Set<String> exist = new HashSet<>();
        String[] fieldMapping = StringUtil.split(fieldMappings, StringUtil.COMMA);
        for (String mapping : fieldMapping) {
            String[] m = StringUtil.split(mapping, StringUtil.VERTICAL_LINE);
            if (m.length == 2) {
                String sName = m[0];
                String tName = m[1];
                if (!exist.contains(mapping) && sMap.containsKey(sName) && tMap.containsKey(tName)) {
                    fieldMappingList.add(new FieldMapping(sMap.get(sName), tMap.get(tName)));
                    exist.add(mapping);
                }
                continue;
            }

            // |C2,C3|
            if (m.length == 1) {
                String name = m[0];
                if (StringUtil.startsWith(mapping, StringUtil.VERTICAL_LINE)) {
                    if (!exist.contains(mapping)) {
                        tMap.computeIfPresent(name, (k, field) -> {
                            fieldMappingList.add(new FieldMapping(null, field));
                            exist.add(mapping);
                            return field;
                        });
                    }
                    continue;
                }
                if (!exist.contains(mapping)) {
                    sMap.computeIfPresent(name, (k, field) -> {
                        fieldMappingList.add(new FieldMapping(field, null));
                        exist.add(mapping);
                        return field;
                    });
                }
            }
        }
        exist.clear();
    }

    private void shuffleColumn(List<Field> col, Map<String, Field> map) {
        col.forEach(f -> map.putIfAbsent(f.getName().toUpperCase(), f));
    }

    /**
     * 解析映射关系
     *
     * @param tableGroup
     * @param json       [{"source":"id","target":"id"}]
     * @return
     */
    public void setFieldMapping(TableGroup tableGroup, String json) {
        List<Map> mappings = JsonUtil.parseList(json);
        if (null == mappings) {
            throw new BizException("映射关系不能为空");
        }

        final Map<String, Field> sMap = PickerUtil.convert2Map(tableGroup.getSourceTable().getColumn());
        final Map<String, Field> tMap = PickerUtil.convert2Map(tableGroup.getTargetTable().getColumn());
        int length = mappings.size();
        List<FieldMapping> list = new ArrayList<>();
        Map row = null;
        Field s = null;
        Field t = null;
        for (int i = 0; i < length; i++) {
            row = mappings.get(i);
            String srcKey = getFieldKey(row, "source");
            String tgtKey = getFieldKey(row, "target");
            s = StringUtil.isNotBlank(srcKey) ? sMap.get(srcKey) : null;
            t = StringUtil.isNotBlank(tgtKey) ? tMap.get(tgtKey) : null;
            if (isRelTableField(s) && StringUtil.isNotBlank(tgtKey)) {
                t = createTargetChildField(tableGroup, tgtKey);
            }
            if (null == s && null == t) {
                continue;
            }
            if (null != t) {
                t.setPk((Boolean) row.get("pk"));
            }
            list.add(new FieldMapping(s, t));
        }
        tableGroup.setFieldMapping(list);
    }

    /**
     * 抽取公共方法：从映射行中获取字段key
     */
    private String getFieldKey(Map mappingRow, String key) {
        Object value = mappingRow.get(key);
        return ObjectUtils.isEmpty(value) ? null : String.valueOf(value);
    }

    private boolean isRelTableField(Field field) {
        if (field == null || field.getTypeName() == null) {
            return false;
        }
        return DataTypeEnum.isRelTable(field.getTypeName());
    }

    /**
     * RELTABLE 类型源字段的目标为同 mapping 下其它子表映射的 {@link TableGroup#getId()}，非当前表目标列。
     * 兼容历史：曾把目标表名写入 target 的仍可匹配并规范为子表映射 id。
     */
    private Field createTargetChildField(TableGroup current, String childTableGroupId) {
        List<TableGroup> all = profileComponent.getTableGroupAll(current.getMappingId());
        for (TableGroup g : all) {
            //排除掉当前的映射主表，避免死循环
            if (g.getId().equals(current.getId())) {
                continue;
            }
            if (childTableGroupId.equals(g.getId())) {
                return new Field(g.getId(), DataTypeEnum.RELTABLE.name(), DataTypeEnum.RELTABLE.ordinal());
            }
        }
        throw new BizException("嵌套字段映射目标无效，请选择同驱动下的子表映射");
    }

}