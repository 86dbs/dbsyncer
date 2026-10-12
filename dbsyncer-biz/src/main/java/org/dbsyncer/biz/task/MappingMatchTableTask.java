/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.task;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.common.dispatch.AbstractDispatchTask;
import org.dbsyncer.common.dispatch.DispatchTaskService;
import org.dbsyncer.common.enums.DispatchTaskEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.MappingProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * 异步写入表映射（匹配相似表或自定义 tableGroups），完成后提交 MappingCountTask。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/4/8
 */
@Service
public final class MappingMatchTableTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String mappingId;

    /**
     * 非空时按自定义文本映射；为空时按源/目标同名表自动匹配。
     */
    private String tableGroups;

    @Resource
    private TableGroupService tableGroupService;

    @Resource
    private MappingProfile mappingProfile;

    @Resource
    private DispatchTaskService dispatchTaskService;

    @Resource
    private MappingCountTask mappingCountTask;

    @Override
    public DispatchTaskEnum getType() {
        return DispatchTaskEnum.MAPPING_MATCH_TABLE;
    }

    @Override
    public String getUniqueId() {
        return mappingId;
    }

    @Override
    public void execute() {
        Mapping mapping = mappingProfile.get(mappingId);
        if (mapping == null) {
            logger.warn("Mapping not found, skip match table, mappingId={}", mappingId);
            return;
        }
        int added;
        if (StringUtil.isNotBlank(tableGroups)) {
            added = matchCustomizedTableGroups(mapping, tableGroups);
            logger.info("自定义表映射完成:{}, 新增{}张表映射", mapping.getName(), added);
        } else {
            added = matchSimilarTableGroups(mapping);
            logger.info("自动匹配完成:{}, 新增{}张表映射", mapping.getName(), added);
        }

        MappingCountTask task = (MappingCountTask) mappingCountTask.clone();
        task.setMappingId(mapping.getId());
        dispatchTaskService.execute(task);
    }

    /**
     * 批量写入相似表映射，不逐表提交 CountTask。
     *
     * @return 实际新增条数
     */
    private int matchSimilarTableGroups(Mapping mapping) {
        List<Table> sourceTables = mapping.getSourceTable();
        List<Table> targetTables = mapping.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            return 0;
        }
        Map<String, Table> targetTableMap = targetTables.stream()
                .collect(Collectors.toMap(table -> table.getName().toUpperCase(), table -> table, (a, b) -> a));
        List<String> sourceNames = new ArrayList<>();
        List<String> targetNames = new ArrayList<>();
        for (Table sourceTable : sourceTables) {
            if (sourceTable == null || StringUtil.isBlank(sourceTable.getName())) {
                continue;
            }
            Table targetTable = targetTableMap.get(sourceTable.getName().toUpperCase());
            if (targetTable == null || !TableTypeEnum.isTable(targetTable.getType())) {
                continue;
            }
            sourceNames.add(sourceTable.getName());
            targetNames.add(targetTable.getName());
        }
        if (sourceNames.isEmpty()) {
            return 0;
        }
        Map<String, String> params = new HashMap<>();
        params.put("mappingId", mapping.getId());
        params.put("sourceTable", StringUtil.join(sourceNames, StringUtil.VERTICAL_LINE));
        params.put("targetTable", StringUtil.join(targetNames, StringUtil.VERTICAL_LINE));
        params.put("sourceType", TableTypeEnum.TABLE.getCode());
        params.put("targetType", TableTypeEnum.TABLE.getCode());
        return tableGroupService.addWithoutCount(params);
    }

    /**
     * 按自定义文本批量落库（每行可含独立 fieldMappings）。
     */
    private int matchCustomizedTableGroups(Mapping mapping, String tableGroupsText) {
        List<Table> sourceTables = mapping.getSourceTable();
        List<Table> targetTables = mapping.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            return 0;
        }
        String[] lines = StringUtil.split(tableGroupsText, StringUtil.BREAK_LINE);
        List<Map<String, String>> paramsList = new ArrayList<>();
        // 数据源表|目标源表=源表字段A1*|目标字段A2*
        for (String line : lines) {
            if (StringUtil.isBlank(line)) {
                continue;
            }
            String[] tableGroup = StringUtil.split(line, StringUtil.EQUAL);
            String[] tableGroupNames = StringUtil.split(tableGroup[0], StringUtil.VERTICAL_LINE);
            if (tableGroupNames.length != 2) {
                continue;
            }
            try {
                paramsList.add(buildTableGroupParams(mapping.getId(), tableGroupNames[0], tableGroupNames[1],
                        tableGroup.length == 2 ? tableGroup[1] : StringUtil.EMPTY));
            } catch (SdkException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (CollectionUtils.isEmpty(paramsList)) {
            return 0;
        }
        return tableGroupService.addWithoutCountBatch(paramsList);
    }

    private Map<String, String> buildTableGroupParams(String id, String sourceTableName, String targetTableName, String fieldMappings) {
        Map<String, String> params = new HashMap<>();
        params.put("mappingId", id);
        params.put("sourceTable", sourceTableName);
        params.put("targetTable", targetTableName);
        params.put("sourceType", TableTypeEnum.TABLE.getCode());
        params.put("targetType", TableTypeEnum.TABLE.getCode());
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
        return params;
    }

    private String replaceStar(String name, StringJoiner joiner) {
        if (StringUtil.endsWith(name, StringUtil.STAR)) {
            name = StringUtil.replace(name.trim(), StringUtil.STAR, StringUtil.EMPTY);
            joiner.add(name);
        }
        return name;
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public void setTableGroups(String tableGroups) {
        this.tableGroups = tableGroups;
    }

}
