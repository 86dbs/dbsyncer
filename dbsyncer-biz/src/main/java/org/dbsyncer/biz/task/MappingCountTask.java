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
 * 统计驱动总数任务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-13 00:00
 */
public class MappingCountTask extends AbstractCountTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String getUniqueId() {
        return mappingId;
    }

    @Override
    public DispatchTaskEnum getType() {
        return DispatchTaskEnum.MAPPING_COUNT;
    }

    @Override
    public void execute() {
        Mapping mapping = profileComponent.getMapping(mappingId);
        if (shouldStop(mapping)) {
            return;
        }
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(mappingId);
        logger.info("正在统计:{}, {}张表", mapping.getName(), groupAll.size());

        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        if (CollectionUtils.isEmpty(groupAll)) {
            meta.updateFullTotal();
            return;
        }
        for (TableGroup tableGroup : groupAll) {
            // 驱动任务类型发生切换，提前释放任务
            if (shouldStop(mapping)) {
                logger.warn("驱动被修改, 提前结束任务 ({},{})", mapping.getName(), mapping.getModel());
                return;
            }
            mapping = profileComponent.getMapping(mappingId);
            updateTableGroupCount(mapping, tableGroup);
        }
        meta.updateFullTotal();
        logger.info("完成统计:{}, {}张表", mapping.getName(), groupAll.size());
    }
}