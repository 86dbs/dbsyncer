/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.model.ClusterNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 集群控制服务：节点身份、在线列表、任务级调度。
 * <p>无全局 Leader。任意节点可写配置、启动任务；启动节点选出 Scheduler，调度权在任务上。
 * 单机：是否本机恒 true，调度方法空操作。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public interface ClusterService {

    /**
     * 当前运行时是否启用集群能力（授权、开关、存储等门控）。默认 false。
     *
     * @param licenseService 许可证服务，可空
     * @param clusterEnabled 配置开关 {@code dbsyncer.cluster.enabled}
     * @param storageType    存储类型 {@code dbsyncer.storage.type}
     * @return true 使用本实现作为集群控制面
     */
    default boolean isClusterRuntime(LicenseService licenseService, boolean clusterEnabled, String storageType) {
        return false;
    }

    /**
     * 是否单机部署。
     *
     * @return true 单机
     */
    boolean isStandalone();

    /**
     * 本节点 ID。
     *
     * @return 节点 ID
     */
    String getLocalNodeId();

    /**
     * 集群节点列表。
     *
     * @return 节点，单机为空列表
     */
    List<ClusterNode> listNodes();

    /**
     * 分页查询集群节点。顺序固定为创建时间升序、节点 ID 升序。
     * <p>默认基于 {@link #listNodes()} 内存分页；存储侧可覆盖为库表分页。
     *
     * @param pageNum  页码（从 1 起）
     * @param pageSize 每页条数
     * @return 分页结果
     */
    default Paging<ClusterNode> queryNodes(int pageNum, int pageSize) {
        int safePageNum = Math.max(1, pageNum);
        int safePageSize = Math.max(1, pageSize);
        List<ClusterNode> source = listNodes();
        List<ClusterNode> all = new ArrayList<>(source == null ? Collections.emptyList() : source);
        all.sort(Comparator
                .comparingLong(ClusterNode::getCreateTime)
                .thenComparing(n -> StringUtil.getIfBlank(n.getNodeId(), StringUtil.EMPTY), String::compareTo));
        Paging<ClusterNode> paging = new Paging<>(safePageNum, safePageSize);
        paging.setTotal(all.size());
        int offset = (safePageNum - 1) * safePageSize;
        if (offset >= all.size()) {
            paging.setData(Collections.emptyList());
            return paging;
        }
        paging.setData(all.stream().skip(offset).limit(safePageSize).collect(Collectors.toList()));
        return paging;
    }

    /**
     * 在线节点。
     *
     * @return 在线节点
     */
    default List<ClusterNode> listOnlineNodes() {
        return Collections.emptyList();
    }

    /**
     * 启动任务：选出 Scheduler、写入调度并通知目标节点。
     * <p>全量+增量从启动起即绑定同一 Scheduler，切增量不再二次分配。
     *
     * @param taskId 任务 / Mapping ID
     * @param model  同步方式（{@link org.dbsyncer.sdk.enums.ModelEnum} code）
     * @return true 本机是 Scheduler，应拉起对应 Puller
     */
    default boolean prepareTaskStart(String taskId, String model) {
        return true;
    }

    /**
     * 本机是否为该任务的 Scheduler。单机恒 true。
     *
     * @param taskId 任务 / Mapping ID
     * @return true 应在本节点执行
     */
    default boolean isTaskAssignedToLocal(String taskId) {
        return true;
    }

    /**
     * 停止任务调度：置空 Scheduler 并通知原节点 stopLocal。单机空操作。
     *
     * @param taskId 任务 / Mapping ID
     */
    default void clearTaskSchedule(String taskId) {
    }

    /**
     * 本机围栏：该任务是否仍应由本机执行（节点与 epoch 仍匹配）。单机恒 true。
     *
     * @param taskId 任务 / Mapping ID
     * @return true 允许本机继续执行
     */
    default boolean assertTaskWritable(String taskId) {
        return true;
    }

    /**
     * 内部拉起：校验本机仍持有该任务后启动执行器。单机默认 true。
     *
     * @param taskId 任务 / Mapping ID
     * @param epoch  调度代数
     * @return true 已拉起或本机无需执行
     */
    default boolean executeLocal(String taskId, int epoch) {
        return true;
    }

    /**
     * 内部停止：仅停本机执行器，不改调度行。
     *
     * @param taskId 任务 / Mapping ID
     */
    default void stopExecuteLocal(String taskId) {
    }

    /**
     * 从集群移除节点。
     *
     * @param nodeId 节点 ID
     */
    default void removeNode(String nodeId) {
        throw new SdkException("单机模式不支持移除节点");
    }

    /**
     * 修改节点展示名称。
     *
     * @param nodeId 节点 ID
     * @param name   展示名称
     */
    default void updateNodeName(String nodeId, String name) {
        throw new SdkException("单机模式不支持修改节点名称");
    }
}
