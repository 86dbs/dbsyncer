/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.TableGroup;

import java.util.List;

/**
 * 表映射关系配置（dbsyncer_table_group）操作。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface TableGroupProfile {

    /**
     * 添加 TableGroup，并同路径预建明细 Meta（id=taskId=tableGroupId）。
     */
    String addTableGroup(TableGroup model);

    /**
     * 批量添加 TableGroup，并同路径批量预建明细 Meta。
     *
     * @param models TableGroup 列表
     */
    void addTableGroupBatch(List<TableGroup> models);

    String editTableGroup(TableGroup model);

    /**
     * 删除单个 TableGroup，并同路径删除其明细 Meta。
     */
    void removeTableGroup(String id);

    /**
     * 按任务 ID 条件删除全部 table_group 及其明细 Meta（先取 id 删 Meta，再 delete WHERE TASK_ID）。
     */
    void removeTableGroupsByTaskId(String taskId);

    TableGroup getTableGroup(String tableGroupId);

    List<TableGroup> getTableGroupAll(String mappingId);

    List<TableGroup> getSortedTableGroupAll(String mappingId);

    /**
     * 获取未完成的 tableGroups 数据
     *
     * @param taskId   任务 ID
     * @param pageNum  页码（从 1 起）
     * @param pageSize 每页条数
     * @return 未完成的 TableGroup 列表（无则空列表）
     */
    List<TableGroup> getIncompleteTableGroups(String taskId, int pageNum, int pageSize);

    int getTableGroupCount(String mappingId);

    List<String> listTableGroupIds(String taskId);
}
