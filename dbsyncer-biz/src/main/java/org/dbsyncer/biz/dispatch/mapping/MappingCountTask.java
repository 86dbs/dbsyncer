/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.dispatch.mapping;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.dispatch.AbstractDispatchTask;
import org.dbsyncer.biz.enums.TaskSchedulerEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.ParserComponent;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.PickerUtil;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 统计驱动总数任务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-13 00:00
 */
public class MappingCountTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String mappingId;

    private String metaSnapshot;

    private ParserComponent parserComponent;

    private ProfileComponent profileComponent;

    private TableGroupService tableGroupService;

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
        logger.info("正在统计驱动总数 ({})", mappingId);
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(mappingId);
        if (!CollectionUtils.isEmpty(groupAll)) {
            Mapping mapping = profileComponent.getMapping(mappingId);
            for (TableGroup tableGroup : groupAll) {
                // 驱动任务类型发生切换，提前释放任务
                if (!ModelEnum.isFull(mapping.getModel())) {
                    logger.warn("驱动类型已切换, 提前结束任务 ({},{})", mapping.getName(), mapping.getModel());
                    break;
                }
                mapping = profileComponent.getMapping(mappingId);
                // 合并高级配置
                TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
                Map<String, String> command = parserComponent.getCommand(mapping, group);
                long count = parserComponent.getCount(mapping.getSourceConnectorId(), command);
                tableGroup.getSourceTable().setCount(count);
                profileComponent.editConfigModel(tableGroup);
                logger.info("完成统计[{}], 表[{}]总数:{}", mapping.getName(), tableGroup.getSourceTable().getName(), count);
            }
            // 更新驱动meta
            tableGroupService.updateMeta(mapping, metaSnapshot);
            logger.info("完成统计[{}], [{}]张表", mapping.getName(), groupAll.size());
        }
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public void setMetaSnapshot(String metaSnapshot) {
        this.metaSnapshot = metaSnapshot;
    }

    public void setParserComponent(ParserComponent parserComponent) {
        this.parserComponent = parserComponent;
    }

    public void setProfileComponent(ProfileComponent profileComponent) {
        this.profileComponent = profileComponent;
    }

    public void setTableGroupService(TableGroupService tableGroupService) {
        this.tableGroupService = tableGroupService;
    }
}