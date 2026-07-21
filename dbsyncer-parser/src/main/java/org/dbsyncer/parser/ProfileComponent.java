/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;

import java.util.List;
import java.util.Map;

/**
 * 配置文件组件（system/user/connector/mapping/tableGroup/meta）
 * <p>
 * {@link ConfigConstant}
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-13 20:48
 */
public interface ProfileComponent {

    /**
     * 解析连接器配置为Connector
     *
     * @param json
     * @return
     */
    Connector parseConnector(String json);

    /**
     * 解析配置
     *
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    <T> T parseObject(String json, Class<T> clazz);

    /**
     * 添加ConfigModel
     *
     * @param model
     * @return id
     */
    String addConfigModel(ConfigModel model);

    /**
     * 编辑ConfigModel
     *
     * @param model
     * @return id
     */
    String editConfigModel(ConfigModel model);

    /**
     * 刪除ConfigModel
     *
     * @param id
     * @return
     */
    void removeConfigModel(String id);

    /**
     * 获取系统配置
     *
     * @return
     */
    SystemConfig getSystemConfig();

    /**
     * 获取用户配置
     *
     * @return
     */
    UserConfig getUserConfig();

    /**
     * 根据ID获取连接器
     *
     * @param connectorId
     * @return
     */
    Connector getConnector(String connectorId);

    /**
     * 获取所有的连接器
     *
     * @return
     */
    List<Connector> getConnectorAll();

    // Mapping
    Mapping getMapping(String mappingId);

    List<Mapping> getMappingAll();

    // TableGroup
    String addTableGroup(TableGroup model);

    /**
     * 批量添加 TableGroup（单次存储批量写入）。
     *
     * @param models TableGroup 列表
     * @return 已添加的 id 列表（与入参顺序一致）
     */
    List<String> addTableGroupBatch(List<TableGroup> models);

    String editTableGroup(TableGroup model);

    void removeTableGroup(String id);

    TableGroup getTableGroup(String tableGroupId);

    List<TableGroup> getTableGroupAll(String mappingId);

    List<TableGroup> getSortedTableGroupAll(String mappingId);

    int getTableGroupCount(String mappingId);

    // Meta
    Meta getMeta(String metaId);

    /**
     * 全部 Meta（含明细级）。优先使用 {@link #getTaskMetaAll()}。
     */
    List<Meta> getMetaAll();

    /**
     * 仅任务级 Meta（IS_TASK_DETAIL=0）。
     *
     * @return 任务级 Meta 列表
     */
    List<Meta> getTaskMetaAll();

    /**
     * 按关联 ID + 是否明细查询 Meta（任务级：taskId=任务ID；明细级：taskId=detailId 或 tableGroupId）。
     *
     * @param refId        关联 ID
     * @param isTaskDetail 0-任务级 1-明细级
     * @return Meta，不存在时返回 null
     */
    Meta getMetaByRefId(String refId, int isTaskDetail);

    /**
     * 批量按关联 ID 查询明细级 Meta（IS_TASK_DETAIL=1）。
     *
     * @param refIds 关联 ID 列表(detailId / tableGroupId)
     * @return key=refId
     */
    Map<String, Meta> getDetailMetaMap(List<String> refIds);

    /**
     * 删除任务明细分表对应的明细级 Meta（须在 clear 分表之前调用）。
     *
     * @param taskId 任务 ID
     */
    void removeDetailMetasByTaskId(String taskId);

    /**
     * 明细分表按成功标记 COUNT。
     *
     * @param taskId    任务 ID
     * @param isSuccess 0-失败 1-成功
     * @return 行数
     */
    long countTaskDetailBySuccess(String taskId, int isSuccess);

    /**
     * 统计明细级 Meta 中 DIFF&gt;0 的数量。
     *
     * @param taskId 任务 ID
     * @return 有差异明细数
     */
    long countDetailMetaWithPositiveDiff(String taskId);

    /**
     * Meta 计数原子增量(严格走库)：直接落库自增 total/success/fail，可为负数。
     *
     * @param metaId       任务ID
     * @param totalDelta   总数增量
     * @param successDelta 成功数增量
     * @param failDelta    失败数增量
     */
    void incrementMeta(String metaId, long totalDelta, long successDelta, long failDelta);

    /**
     * 构建导出配置快照(直查库)，用于配置导入/导出。
     *
     * @return 快照
     */
    Map<String, Object> getConfigSnapshot();

    /**
     * 获取所有条件类型
     *
     * @return
     */
    List<OperationEnum> getOperationEnumAll();

    /**
     * 获取过滤条件系统参数
     *
     * @return
     */
    List<QuartzFilterEnum> getQuartzFilterEnumAll();

    /**
     * 获取所有运算符类型
     *
     * @return
     */
    List<FilterEnum> getFilterEnumAll();

    /**
     * 获取所有转换类型
     *
     * @return
     */
    List<ConvertEnum> getConvertEnumAll();

    /**
     * 获取所有同步数据状态类型
     *
     * @return
     */
    List<StorageDataStatusEnum> getStorageDataStatusEnumAll();
}
