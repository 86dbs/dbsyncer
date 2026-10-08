/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.Mapping;

import java.util.List;
import java.util.function.Consumer;

/**
 * 同步任务
 *
 * @author wuji
 * @version 1.0.0
 */
public interface MappingProfile {

    /**
     * 按 id 查询 Mapping 任务配置。
     */
    Mapping get(String id);

    /**
     * 新增任务配置。
     */
    String add(Mapping mapping);

    /**
     * 批量新增任务配置。
     */
    void addBatch(List<Mapping> mappings);

    /**
     * 修改任务配置。
     */
    String update(Mapping mapping);

    /**
     * 删除任务配置
     */
    void delete(String id);

    void clearRunData(String id);

    /**
     * 删除任务时清理运行数据（物理 DROP 明细分表）。
     */
    void dropTaskDetailTable(String id);

    /**
     * 按模型类型分页查询任务，可选按名称模糊搜索。
     *
     * @param searchKey 可选；非空时对 {@code name} 做 LIKE
     */
    Paging<Mapping> query(int pageNum, int pageSize, String searchKey);

    /**
     * 按模型类型分页回调遍历全部任务。
     */
    void pageScanTasks(int pageSize, Consumer<List<Mapping>> pageConsumer);

}
