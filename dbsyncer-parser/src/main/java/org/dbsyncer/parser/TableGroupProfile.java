/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.storage.SqlQuery;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * 表映射关系配置（dbsyncer_table_group）操作。
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
     * 批量导入表映射（不预建表级 Meta，供配置包还原）。
     */
    void importTableGroupBatch(List<TableGroup> models);

    /**
     * 从 NDJSON 行批量导入（内部按批次刷库）。
     */
    void importTableGroupNdjsonLines(List<String> ndjsonLines);

    /**
     * 从 ZIP 导入 table_group/*.ndjson。
     */
    void importFromZip(ZipFile zip) throws IOException;

    /**
     * 导出 table_group 到 ZIP（按 taskId 分 NDJSON 文件）。
     */
    int writeTableGroupsToZip(ZipOutputStream zos) throws IOException;

    /**
     * 按 taskId 升序遍历全部表映射（ZIP 导出 NDJSON）。
     */
    void pageScanTableGroupsByTaskId(Consumer<TableGroup> consumer);

    /**
     * 旧版 JSON 导入/导出快照中，任务下 table_group 分组键（{@code tableGroup_{taskId}}）。
     */
    String getPreloadGroupKey(String taskId);
}
