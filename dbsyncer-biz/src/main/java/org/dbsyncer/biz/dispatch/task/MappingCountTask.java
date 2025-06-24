/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.dispatch.task;

import org.dbsyncer.biz.dispatch.AbstractCountTask;
import org.dbsyncer.biz.enums.TaskSchedulerEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 统计驱动总数任务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-13 00:00
 */
public class MappingCountTask extends AbstractCountTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String metaSnapshot;

    @Override
    public String getUniqueId() {
        return mappingId;
    }

    @Override
    public TaskSchedulerEnum getType() {
        return TaskSchedulerEnum.MAPPING_COUNT;
    }

    @Override
    public void execute() {
        Mapping mapping = profileComponent.getMapping(mappingId);
        if (shouldStop(mapping)) {
            return;
        }
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(mappingId);
        logger.info("正在统计:{}, {}张表", mapping.getName(), groupAll.size());
        if (!CollectionUtils.isEmpty(groupAll)) {
            for (TableGroup tableGroup : groupAll) {
                // 驱动任务类型发生切换，提前释放任务
                if (shouldStop(mapping)) {
                    logger.warn("驱动被修改, 提前结束任务 ({},{})", mapping.getName(), mapping.getModel());
                    return;
                }
                mapping = profileComponent.getMapping(mappingId);
                updateTableGroupCount(mapping, tableGroup);
            }
            // 更新驱动meta
            Meta meta = tableGroupService.updateMeta(mapping, metaSnapshot);
            logger.info("完成统计:{}, {}张表, 总数:{}", mapping.getName(), groupAll.size(), meta.getTotal());
        }
    }

    public void setMetaSnapshot(String metaSnapshot) {
        this.metaSnapshot = metaSnapshot;
    }
}