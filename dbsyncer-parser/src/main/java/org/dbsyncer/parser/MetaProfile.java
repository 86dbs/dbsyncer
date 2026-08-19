/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableSyncProgress;
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
     * 合并单表进度：乐观锁 CAS（按 updateTime）+ 单调前进校验，避免多节点整行互盖。
     *
     * @param metaId       任务级 Meta 主键
     * @param tableGroupId 表映射 ID / WorkItem ID
     * @param progress     候选进度（含 generation）
     * @return true 已合并；false 被拒绝或冲突耗尽
     */
    boolean mergeTableProgress(String metaId, String tableGroupId, TableSyncProgress progress);

    /**
     * 合并进度并在同一条 CAS 中累加 success/fail，避免进度已推进而计数未落盘。
     *
     * @param metaId       任务级 Meta 主键
     * @param tableGroupId 表映射 ID / WorkItem ID
     * @param progress     候选进度
     * @param successDelta 本页成功条数
     * @param failDelta    本页失败条数
     * @return true 已合并
     */
    boolean mergeTableProgress(String metaId, String tableGroupId, TableSyncProgress progress,
                               long successDelta, long failDelta);

    /**
     * 仅更新 Meta.state，不覆盖 snapshot。
     *
     * @param metaId Meta 主键
     * @param state  目标状态
     * @return true 更新成功
     */
    boolean updateMetaState(String metaId, int state);

    /**
     * 仅当 START_TIME 仍为 0 时写入启动时间，不触碰 success/snapshot。
     *
     * @param metaId    Meta 主键
     * @param startTime 启动时间毫秒
     * @return true 已写入或无需写入
     */
    boolean ensureStartTime(String metaId, long startTime);

    /**
     * 将表内 range 计划 CAS 写入 SNAPSHOT；已有计划则保持不变。不覆盖 success/fail。
     *
     * @param metaId       任务级 Meta 主键
     * @param tableGroupId 表映射 ID
     * @param itemIds      完整 range item 列表
     * @return true 已写入或已存在
     */
    boolean mergeRangePlan(String metaId, String tableGroupId, List<String> itemIds);

    /**
     * 将 total 对齐为 success+fail（全量结束后消除 COUNT 预估偏差）。
     *
     * @param metaId Meta 主键
     * @return true 已对齐或无需对齐
     */
    boolean alignMetaTotalToProcessed(String metaId);

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
