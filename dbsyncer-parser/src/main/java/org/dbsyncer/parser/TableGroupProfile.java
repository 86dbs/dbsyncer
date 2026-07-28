/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.storage.SqlQuery;

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
     * 按 SQL 查询表映射（透传执行，结果按 TableGroup 反序列化）。
     *
     * @param query SQL 与可选分页参数
     * @return TableGroup 列表（无则空列表）
     */
    List<TableGroup> getIncompleteTableGroups(SqlQuery query);

    int getTableGroupCount(String mappingId);

    List<String> listTableGroupIds(String taskId);
}
