/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.TaskImportResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.ZipFile;

/**
 * 任务域统一入口
 * 以及任务运行结果（Meta、TASK_DETAIL）的清理与重置。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface TaskProfile {


    /**
     * 按 id 查询一条任务配置。
     */
    <T extends ConfigModel> T getTask(String id, Class<T> clazz);

    /**
     * 按模型类型分页查询任务，可选按名称模糊搜索。
     *
     * @param searchKey 可选；非空时对 {@code name} 做 LIKE
     */
    <T extends ConfigModel> Paging<T> queryTasks(Class<T> clazz, int pageNum, int pageSize, String searchKey);

    /**
     * 按模型类型分页回调遍历全部任务。
     */
    <T extends ConfigModel> void pageScanTasks(Class<T> clazz, int pageSize, Consumer<List<T>> pageConsumer);

    /**
     * 新增任务配置。
     */
    String addTask(ConfigModel task);

    /**
     * 修改任务配置。
     */
    String updateTask(ConfigModel task);

    /**
     * 批量新增任务配置。
     */
    void addTaskBatch(List<? extends ConfigModel> tasks);

    /**
     * 删除任务配置（仅删 task 表行）。
     */
    void deleteTask(String id);

    /**
     * 按 TYPE 统计任务数量。
     */
    int countTasks(String type);

    /**
     * 任务 id 是否存在于 {@code dbsyncer_task}。
     */
    boolean existsTask(String id);

    /**
     * 全部任务行数。
     */
    int countAllTasks();

    /**
     * 导出用：全部任务配置 JSON（Map 形式）。
     */
    List<Map<String, Object>> listAllTaskJsonMaps();

    /**
     * 配置包还原：写入任务行（保留 id；不经 TaskService.add，避免重复建 Meta）。
     */
    String importTask(ConfigModel task);

    /**
     * 从 task.json / mapping.json 数组导入全类型任务；Mapping 批量落库，企业任务逐条 importTask。
     */
    TaskImportResult importTasksFromJson(String json);

    /**
     * 从 task_detail.json 预建空分表（仅结构，无行数据）。
     */
    void importTaskDetailSchemasFromJson(String json);

    /**
     * 从 ZIP 导入 task.json（兼容 mapping.json）。
     */
    TaskImportResult importTasksFromZip(ZipFile zip) throws IOException;

    /**
     * 构建 task_detail.json 内容（仅 taskIds）。
     */
    String exportTaskDetailSchemasJson(List<String> taskIds);

    // ---------- 任务运行结果 ----------

    /**
     * 删除任务下各表映射的运行 Meta（按 table_group.id）。
     */
    void deleteTableRunMeta(String taskId);

    /**
     * 清空任务运行数据：删表级 Meta、清空 TASK_DETAIL；表映射仍在时补回空明细 Meta。
     */
    void clearRunData(String taskId);

    /**
     * 预建运行明细分表 {@code dbsyncer_task_detail_{taskId}}。
     */
    void createRunDetailTable(String taskId);

    /**
     * 批量预建运行明细分表（配置包导入）。
     */
    void createRunDetailTables(List<String> taskIds);

    /**
     * 重置任务级运行进度（计数归零、state=READY，保留 Meta 行）。
     */
    void resetRunProgress(String taskId);
}
