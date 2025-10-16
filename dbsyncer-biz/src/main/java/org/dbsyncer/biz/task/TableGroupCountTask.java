/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.task;

import org.dbsyncer.common.enums.DispatchTaskEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 统计驱动表总数任务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-24 01:23
 */
public class TableGroupCountTask extends AbstractCountTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private List<String> tableGroups;

    @Override
    public void execute() throws Exception {
        Mapping mapping = profileComponent.getMapping(mappingId);
        if (shouldStop(mapping)) {
            return;
        }
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(mappingId);
        logger.info("正在统计:{}, {}张表", mapping.getName(), groupAll.size());
        if (!CollectionUtils.isEmpty(tableGroups)) {
            for (String tableGroupId : tableGroups) {
                // 驱动任务类型发生切换，提前释放任务
                if (shouldStop(mapping)) {
                    logger.warn("驱动被修改, 提前结束任务 ({},{})", mapping.getName(), mapping.getModel());
                    return;
                }
                mapping = profileComponent.getMapping(mappingId);
                updateTableGroupCount(mapping, profileComponent.getTableGroup(tableGroupId));
            }
        }
        Meta meta = mapping.getMeta();
        meta.updateTotal();
        logger.info("完成统计:{}, {}张表", mapping.getName(), groupAll.size());
    }

    @Override
    public String getUniqueId() {
        return CollectionUtils.isEmpty(tableGroups) ? mappingId : Integer.toHexString(tableGroups.hashCode());
    }

    @Override
    public DispatchTaskEnum getType() {
        return DispatchTaskEnum.TABLE_GROUP_COUNT;
    }

    public void setTableGroups(List<String> tableGroups) {
        this.tableGroups = tableGroups;
    }
}
