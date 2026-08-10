/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.storage.SqlQuery;

import java.util.List;
import java.util.function.Consumer;

/**
 * 表映射关系配置（dbsyncer_table_group）操作。
 * <p>配置包 ZIP/NDJSON 的导入导出编排在 biz 的 {@code ConfigImportService}/{@code ConfigExportService}。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface TableGroupProfile {

    /**
     * 添加 TableGroup，并同路径预建表级 Meta（id=雪花，taskId=tableGroupId，isTaskDetail=1）。
     */
    String addTableGroup(TableGroup model);

    /**
     * 批量添加 TableGroup，并同路径批量预建表级 Meta（id=雪花，taskId=tableGroupId）。
     *
     * @param models TableGroup 列表
     */
    void addTableGroupBatch(List<TableGroup> models);

    /**
     * 批量写入 table_group 行（不预建表级 Meta；配置包还原时 Meta 另路径导入）。
     */
    void addTableGroupBatchWithoutMeta(List<TableGroup> models);

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

    /**
     * 按 taskId 分页查询表映射，固定按 sortIndex 降序。
     *
     * @param mappingId 任务/驱动 ID
     * @param searchKey 可选；非空时对 sourceTable/targetTable 做 LIKE（OR）
     * @param pageNum   页码（从 1 起）
     * @param pageSize  每页条数；非法时回落 {@link org.dbsyncer.sdk.constant.ConfigConstant#PAGE_SIZE}
     * @return 分页结果
     */
    Paging<TableGroup> queryTableGroup(String mappingId, String searchKey, int pageNum, int pageSize);

    /**
     * 同步结果详情：table_group LEFT JOIN meta，库侧按更新时间/失败数/成功数降序分页。
     *
     * @param mappingId    驱动 ID
     * @param detailStatus 可选；{@code fail} 仅失败、{@code success} 仅成功（fail=0），空则全部
     * @param pageNum      页码（从 1 起）
     * @param pageSize     每页条数
     * @return 分页行（字段：tableGroupId / sourceTable / targetTable / successTotal / failTotal / updateTime）
     */
    Paging queryTableGroupResults(String mappingId, String detailStatus, int pageNum, int pageSize);

    /**
     * 按页回调遍历任务下全部表映射（页内顺序为 sortIndex 降序）。
     *
     * @param mappingId    任务/驱动 ID
     * @param pageSize     每页条数；非法时回落 {@link org.dbsyncer.sdk.constant.ConfigConstant#PAGE_SIZE}
     * @param pageConsumer 页回调
     */
    void pageScanTableGroups(String mappingId, int pageSize, Consumer<List<TableGroup>> pageConsumer);

    /**
     * 按 SQL 查询表映射（透传执行，结果按 TableGroup 反序列化）。
     * <p>过滤条件由调用方 SQL 决定（例如未完成明细 Meta 连表），本方法不做业务语义过滤。
     *
     * @param query SQL 与可选分页参数
     * @return TableGroup 列表（无则空列表）
     */
    List<TableGroup> listTableGroupsBySql(SqlQuery query);

    int getTableGroupCount(String mappingId);

    /**
     * 是否已存在相同源表+目标表映射（库侧等值查询，不扫全表）。
     */
    boolean existsTableGroup(String taskId, String sourceTable, String targetTable);

    List<String> listTableGroupIds(String taskId);

    /**
     * 表映射总数。
     */
    int countTableGroups();

    /**
     * 按 taskId 升序遍历全部表映射。
     */
    void pageScanTableGroupsByTaskId(Consumer<TableGroup> consumer);

    /**
     * 旧版 JSON 导入/导出快照中，任务下 table_group 分组键（{@code tableGroup_{taskId}}）。
     */
    String getPreloadGroupKey(String taskId);
}
