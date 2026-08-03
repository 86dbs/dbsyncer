/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.task;

import org.dbsyncer.common.enums.DispatchTaskEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

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
    public DispatchTaskEnum getType() {
        return DispatchTaskEnum.MAPPING_COUNT;
    }

    @Override
    public void execute() {
        Mapping mapping = profileComponent.getMapping(mappingId);
        if (shouldStop(mapping)) {
            return;
        }
        int groupCount = tableGroupProfile.getTableGroupCount(mappingId);
        logger.info("正在统计:{}, {}张表", mapping.getName(), groupCount);
        if (groupCount > 0) {
            AtomicReference<Mapping> mappingRef = new AtomicReference<>(mapping);
            tableGroupProfile.forEachTableGroupPage(mappingId, ConfigConstant.PAGE_SIZE, page -> {
                if (CollectionUtils.isEmpty(page)) {
                    return;
                }
                for (TableGroup tableGroup : page) {
                    if (tableGroup == null) {
                        continue;
                    }
                    Mapping current = mappingRef.get();
                    // 驱动任务类型发生切换，提前释放任务
                    if (shouldStop(current)) {
                        logger.warn("驱动被修改, 提前结束任务 ({},{})", current.getName(), current.getModel());
                        return;
                    }
                    current = profileComponent.getMapping(mappingId);
                    mappingRef.set(current);
                    updateTableGroupCount(current, tableGroup);
                }
            });
            // 更新驱动meta
            Meta meta = tableGroupService.updateMeta(mappingRef.get(), metaSnapshot);
            logger.info("完成统计:{}, {}张表, 总数:{}", mappingRef.get().getName(), groupCount, meta.getTotal());
        }
    }

    public void setMetaSnapshot(String metaSnapshot) {
        this.metaSnapshot = metaSnapshot;
    }
}
