/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.task;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.common.enums.DispatchTaskEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * 统计同步任务表总数任务
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-06-24 01:23
 */
@Service
public final class TableGroupCountTask extends AbstractCountTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private TableGroupService tableGroupService;

    private String mappingId;

    private List<String> tableGroups;

    @Override
    public void execute() throws Exception {
        Mapping mapping = taskProfile.getMapping(mappingId);
        int groupCount = tableGroupProfile.getTableGroupCount(mappingId);
        logger.info("正在统计:{}, {}张表", mapping.getName(), groupCount);
        if (!CollectionUtils.isEmpty(tableGroups)) {
            for (String tableGroupId : tableGroups) {
                // 任务类型发生切换，提前释放任务
                if (shouldStop(mappingId)) {
                    return;
                }
                updateTableGroupCount(mapping, tableGroupProfile.getTableGroup(tableGroupId));
            }
        }
        // 更新驱动meta
        Meta meta = tableGroupService.updateMeta(mapping, null);
        logger.info("完成统计:{}, {}张表, 总数:{}", mapping.getName(), groupCount, meta.getTotal());
    }

    @Override
    public String getUniqueId() {
        return CollectionUtils.isEmpty(tableGroups) ? mappingId : Integer.toHexString(tableGroups.hashCode());
    }

    @Override
    public DispatchTaskEnum getType() {
        return DispatchTaskEnum.TABLE_GROUP_COUNT;
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public void setTableGroups(List<String> tableGroups) {
        this.tableGroups = tableGroups;
    }
}
