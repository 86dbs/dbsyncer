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
import java.util.stream.Collectors;

/**
 * 异步匹配相似表，完成后提交同步任务统计任务
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/4/8
 */
@Service
public final class MappingMatchTableTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String mappingId;

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
        int added = matchSimilarTableGroups(mapping);
        logger.info("自动匹配完成:{}, 新增{}张表映射", mapping.getName(), added);

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

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

}
