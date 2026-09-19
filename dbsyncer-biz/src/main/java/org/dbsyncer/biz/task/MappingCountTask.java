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
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * 统计同步任务总数任务
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2025-06-13 00:00
 */
@Service
public final class MappingCountTask extends AbstractCountTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private TableGroupService tableGroupService;

    private String mappingId;

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
        Mapping mapping = taskProfile.getMapping(mappingId);
        int groupCount = tableGroupProfile.getTableGroupCount(mappingId);
        logger.info("正在统计:{}, {}张表", mapping.getName(), groupCount);
        if (groupCount > 0) {
            tableGroupProfile.pageScanTableGroups(mappingId, ConfigConstant.PAGE_SIZE, page -> {
                if (CollectionUtils.isEmpty(page)) {
                    return;
                }
                for (TableGroup tableGroup : page) {
                    if (tableGroup == null) {
                        continue;
                    }
                    // 同步任务类型发生切换，提前释放任务
                    if (shouldStop(mappingId)) {
                        return;
                    }
                    updateTableGroupCount(mapping, tableGroup);
                }
            });
            // 更新meta
            Meta meta = tableGroupService.updateMeta(mapping, metaSnapshot);
            logger.info("完成统计:{}, {}张表, 总数:{}", mapping.getName(), groupCount, meta.getTotal());
        }
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public void setMetaSnapshot(String metaSnapshot) {
        this.metaSnapshot = metaSnapshot;
    }
}
