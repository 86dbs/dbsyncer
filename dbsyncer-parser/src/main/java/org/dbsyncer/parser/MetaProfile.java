/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.model.MetaIncrement;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.ZipOutputStream;

/**
 * 任务执行结果表（dbsyncer_meta）查询与计数操作。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface MetaProfile {

    /**
     * 按 Meta 主键 id 查询
     */
    Meta getMeta(String metaId);

    /**
     * 分页查询 Meta。
     *
     * @param isTaskDetail 可选；null 不过滤，0 任务级，1 明细级
     * @param pageNum      页码（从 1 起）
     * @param pageSize     每页条数；非法时回落
     */
    Paging<Meta> queryMeta(Integer isTaskDetail, int pageNum, int pageSize);

    /**
     * 分页回调遍历 Meta（避免一次性装入内存）。
     *
     * @param isTaskDetail 可选；null 不过滤，0 任务级，1 明细级
     * @param pageSize     每页条数；非法时回落
     * @param pageConsumer 页回调
     */
    void pageScanMetas(Integer isTaskDetail, int pageSize, Consumer<List<Meta>> pageConsumer);

    /**
     * 按关联 ID + 任务层级查询 Meta（任务级：taskId=任务ID；明细级：taskId=table_group.id）。
     */
    Meta getMetaByTaskId(String refId, TaskLevelEnum taskLevelEnum);

    /**
     * 批量按任务 ID 查询任务级 Meta（IS_TASK_DETAIL=0），key=taskId。
     */
    Map<String, Meta> getTaskMetaMap(List<String> taskIds);

    /**
     * 批量按关联 ID 查询明细级 Meta（IS_TASK_DETAIL=1）。
     */
    Map<String, Meta> getDetailMetaMap(List<String> refIds);

    /**
     * Meta 计数原子增量(严格走库)：按 {@link MetaIncrement} 落库自增
     */
    void incrementMeta(MetaIncrement increment);

    /**
     * 更新 Meta 进度
     */
    void updateMetaProgress(String metaId, int state, Map<String, String> snapshot);

    void deleteMetaByTableGroupIds(List<String> tableGroupIds);

    /**
     * 明细分表分片键：任务级 Meta 的 {@code taskId}（任务/Mapping ID）。
     * <p>入参必须为任务级 Meta（{@code isTaskDetail=0}）；表级 Meta 会抛异常。
     */
    String resolveTaskDetailShardId(Meta meta);

    /**
     * 按 Meta 主键解析明细分表分片键（先查 Meta，再取 taskId）。
     */
    String resolveTaskDetailShardId(String metaId);

    /**
     * 添加 Meta。
     */
    String addMeta(Meta meta);

    /**
     * 批量添加 Meta（配置包导入）。
     */
    void addMetaBatch(List<Meta> metas);

    /**
     * 更新 Meta。
     */
    String updateMeta(Meta meta);

    /**
     * 批量更新 Meta（就地重置进度等场景，避免删插放大）。
     */
    void updateMetaBatch(List<Meta> metas);

    /**
     * 删除 Meta。
     */
    void removeMeta(String id);

    /**
     * Meta 总数。
     */
    int countMeta();

    /**
     * 导出全部 Meta 到 ZIP（meta.json 数组，分页流式写出）。
     *
     * @return 写出条数
     */
    int writeMetasToZip(ZipOutputStream zos) throws IOException;

    /**
     * 从 meta.json 数组批量导入。
     */
    void importMetaFromJson(String json);
}
