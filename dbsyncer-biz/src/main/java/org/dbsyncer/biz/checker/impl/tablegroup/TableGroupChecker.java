/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.tablegroup;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PrimaryKeyRequiredException;
import org.dbsyncer.biz.RepeatedTableGroupException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.schema.SchemaResolver;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.*;
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

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) throws Exception {
        logger.info("params:{}", params);
        String mappingId = params.get("mappingId");
        String sourceTable = params.get("sourceTable");
        String targetTable = params.get("targetTable");
        String sourceTablePK = params.get("sourceTablePK");
        String targetTablePK = params.get("targetTablePK");
        String fieldMappings = params.get("fieldMappings");
        Assert.hasText(mappingId, "tableGroup mappingId is empty.");
        Assert.hasText(sourceTable, "tableGroup sourceTable is empty.");
        Assert.hasText(targetTable, "tableGroup targetTable is empty.");
        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "mapping can not be null.");

        // 检查是否存在重复映射关系（新增时排除ID为null）
        checkRepeatedTable(mappingId, sourceTable, targetTable, null);

        // 获取连接器信息
        TableGroup tableGroup = TableGroup.create(mappingId, sourceTable, parserComponent, profileComponent);
        tableGroup.setSourceTable(getTable(mapping.getSourceConnectorId(), sourceTable, sourceTablePK));
        tableGroup.setTargetTable(getTable(mapping.getTargetConnectorId(), targetTable, targetTablePK));

        // 检查主键：源表和目标表都必须有主键
        List<String> sourceTablePrimaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(tableGroup.getSourceTable());
        if (CollectionUtils.isEmpty(sourceTablePrimaryKeys)) {
            throw new PrimaryKeyRequiredException(String.format("数据源表 %s 缺少主键，无法进行数据同步。", sourceTable));
        }
        List<String> targetTablePrimaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(tableGroup.getTargetTable());
        if (CollectionUtils.isEmpty(targetTablePrimaryKeys)) {
            throw new PrimaryKeyRequiredException(String.format("目标表 %s 缺少主键，无法进行数据同步。", targetTable));
        }

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 字段映射关系：如果 fieldMappings 为空，基于源表字段构建 fieldMapping（target 为 null）
        // 同步字段主要参考源库，target 字段是可选的
        if (StringUtil.isNotBlank(fieldMappings)) {
            matchFieldMapping(tableGroup, fieldMappings);
        } else {
            // 如果 fieldMappings 为空，基于源表所有字段构建 fieldMapping（target 为 null）
            buildFieldMappingFromSourceTable(tableGroup);
        }

        return tableGroup;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) throws Exception {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "TableGroupChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
        Assert.notNull(mapping, "mapping can not be null.");
        String fieldMappingJson = params.get("fieldMapping");

        // 新增：支持修改目标表名称
        String newTargetTableName = params.get("targetTable");
        if (StringUtil.isNotBlank(newTargetTableName)) {
            String sourceTableName = tableGroup.getSourceTable().getName();
            String oldTargetTableName = tableGroup.getTargetTable().getName();

            // 如果目标表名称发生变化，需要重新获取目标表信息
            if (!StringUtil.equals(newTargetTableName, oldTargetTableName)) {
                // 检查是否与其他表映射冲突
                checkRepeatedTable(mapping.getId(), sourceTableName, newTargetTableName, id);

                // 获取新目标表的主键信息（保持原有主键配置）
                List<String> targetTablePks = new ArrayList<>();
                if (tableGroup.getTargetTable().getColumn() != null) {
                    targetTablePks = tableGroup.getTargetTable().getColumn().stream()
                            .filter(Field::isPk)
                            .map(Field::getName)
                            .collect(Collectors.toList());
                }
                String targetTablePK = StringUtil.join(targetTablePks, ",");

                // 重新获取目标表信息
                Table newTargetTable = getTable(mapping.getTargetConnectorId(), newTargetTableName, targetTablePK);
                tableGroup.setTargetTable(newTargetTable);
            }
        }

        // 修改基本配置
        this.modifyConfigModel(tableGroup, params);

        // 修改高级配置：过滤条件/转换配置/插件配置
        this.modifySuperConfigModel(tableGroup, params);

        // 字段映射关系：如果 fieldMappingJson 为空，基于源表重新构建 fieldMapping（与添加时逻辑一致）
        // 这样可以确保字段映射与源表结构一致，如果源表结构发生变化，字段映射会自动更新
        if (StringUtil.isNotBlank(fieldMappingJson)) {
            setFieldMapping(tableGroup, fieldMappingJson);
        } else {
            // 如果 fieldMappingJson 为空，基于源表重新构建 fieldMapping（target 为 null）
            // 与添加时的逻辑保持一致，确保字段映射与源表结构一致
            buildFieldMappingFromSourceTable(tableGroup);
        }

        return tableGroup;
    }

    /**
     * 刷新表字段
     */
    public void refreshTableFields(TableGroup tableGroup) throws Exception {
        Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
        Assert.notNull(mapping, "mapping can not be null.");

        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();

        // 添加空值检查，防止NullPointerException
        List<String> sourceTablePks = new ArrayList<>();
        if (sourceTable.getColumn() != null) {
            sourceTablePks = sourceTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        }

        List<String> targetTablePks = new ArrayList<>();
        if (targetTable.getColumn() != null) {
            targetTablePks = targetTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        }

        tableGroup.setSourceTable(getTable(mapping.getSourceConnectorId(), sourceTable.getName(), StringUtil.join(sourceTablePks, ",")));
        tableGroup.setTargetTable(getTable(mapping.getTargetConnectorId(), targetTable.getName(), StringUtil.join(targetTablePks, ",")));
    }

    private Table getTable(String connectorId, String tableName, String primaryKeyStr) throws Exception {
        MetaInfo metaInfo = parserComponent.getMetaInfo(connectorId, tableName);
        Assert.notNull(metaInfo, "无法获取连接器表信息:" + tableName);
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
        return new Table(tableName, metaInfo.getTableType(), metaInfo.getColumn(), metaInfo.getSql(), metaInfo.getIndexType());
    }

    /**
     * 检查是否存在重复映射关系
     *
     * @param mappingId           映射ID
     * @param sourceTable         源表名称
     * @param targetTable         目标表名称
     * @param excludeTableGroupId 排除的表映射ID（编辑时使用，新增时传null）
     */
    private void checkRepeatedTable(String mappingId, String sourceTable, String targetTable, String excludeTableGroupId) throws Exception {
        List<TableGroup> list = profileComponent.getTableGroupAll(mappingId);
        if (!CollectionUtils.isEmpty(list)) {
            for (TableGroup g : list) {
                // 排除自身（编辑时）
                if (StringUtil.isNotBlank(excludeTableGroupId) && StringUtil.equals(excludeTableGroupId, g.getId())) {
                    continue;
                }
                // 数据源表和目标表都存在
                if (StringUtil.equals(sourceTable, g.getSourceTable().getName()) && StringUtil.equals(targetTable, g.getTargetTable().getName())) {
                    final String error = String.format("映射关系已存在.%s > %s", sourceTable, targetTable);
                    logger.error(error);
                    throw new RepeatedTableGroupException(error);
                }
            }
        }
    }

    private void matchFieldMapping(TableGroup tableGroup, String fieldMappings) {
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

    /**
     * 解析映射关系
     *
     * @param tableGroup
     * @param json       [{"source":"id","target":"id"}]
     * @return
     */
    private void setFieldMapping(TableGroup tableGroup, String json) {
        List<Map<String, Object>> mappings = JsonUtil.parseList(json);
        if (null == mappings) {
            throw new BizException("映射关系不能为空");
        }

        final Map<String, Field> sMap = PickerUtil.convert2Map(tableGroup.getSourceTable().getColumn());
        final Map<String, Field> tMap = PickerUtil.convert2Map(tableGroup.getTargetTable().getColumn());
        int length = mappings.size();
        List<FieldMapping> list = new ArrayList<>();
        Map<String, Object> row = null;
        Field s = null;
        Field t = null;
        for (int i = 0; i < length; i++) {
            row = mappings.get(i);
            s = sMap.get(row.get("source"));
            t = tMap.get(row.get("target"));
            if (null == s && null == t) {
                continue;
            }
            // 用源字段信息作为目标字段信息，但需要进行类型标准化和转换
            if (null == t) {
                // 获取源连接器和目标连接器的SchemaResolver
                Mapping mapping = profileComponent.getMapping(tableGroup.getMappingId());
                ConnectorConfig sourceConnectorConfig = getConnectorConfig(mapping.getSourceConnectorId());
                ConnectorConfig targetConnectorConfig = getConnectorConfig(mapping.getTargetConnectorId());
                ConnectorService<?, ?> sourceConnectorService = connectorFactory.getConnectorService(sourceConnectorConfig.getConnectorType());
                ConnectorService<?, ?> targetConnectorService = connectorFactory.getConnectorService(targetConnectorConfig.getConnectorType());
                SchemaResolver sourceSchemaResolver = sourceConnectorService.getSchemaResolver();
                SchemaResolver targetSchemaResolver = targetConnectorService.getSchemaResolver();

                // 1. 先用源连接器将源字段标准化为Java标准类型
                Field standardField = sourceSchemaResolver.toStandardType(s);

                // 2. 再用目标连接器将Java标准类型转换为目标数据库类型
                if (null == targetSchemaResolver) // 如下游是 kafka
                    t = standardField;
                else
                    t = targetSchemaResolver.fromStandardType(standardField);
            }

            if (null != t) {
                t.setPk((Boolean) row.get("pk"));
            }
            list.add(new FieldMapping(s, t));
        }
        tableGroup.setFieldMapping(list);
    }

    /**
     * 基于源表字段构建 fieldMapping（target 为 null）
     * 同步字段主要参考源库，target 字段是可选的
     *
     * @param tableGroup 表映射组
     */
    private void buildFieldMappingFromSourceTable(TableGroup tableGroup) {
        List<Field> sCol = tableGroup.getSourceTable().getColumn();

        if (CollectionUtils.isEmpty(sCol)) {
            tableGroup.setFieldMapping(new ArrayList<>());
            return;
        }

        // 基于源表所有字段构建 fieldMapping，target 为 null
        // 同步字段主要参考源库，target 字段可以在后续编辑时配置
        List<FieldMapping> fieldMappingList = new ArrayList<>();
        for (Field sourceField : sCol) {
            fieldMappingList.add(new FieldMapping(sourceField, null));
        }

        tableGroup.setFieldMapping(fieldMappingList);
    }

    private ConnectorConfig getConnectorConfig(String connectorId) {
        return profileComponent.getConnector(connectorId).getConfig();
    }

}