/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.model.Table;

import java.util.List;
import java.util.Map;

public interface ValidateSyncService {

    /**
     * 获取任务
     *
     */
    ValidateSyncTaskVO get(String id);

    /**
     * 添加任务
     *
     */
    String add(Map<String, String> params);

    /**
     * 修改任务
     *
     */
    String edit(Map<String, String> params);

    /**
     * 复制任务
     *
     */
    String copy(String id);

    /**
     * 删除任务
     *
     */
    String delete(String id);

    /**
     * 启动任务
     *
     */
    String start(String id);

    /**
     * 停止任务
     *
     */
    String stop(String id);

    /**
     * 搜索任务
     *
     */
    Paging<ValidateSyncTaskVO> search(Map<String, String> params);

    /**
     * 搜索表关系
     *
     */
    Paging<TableGroup> searchTableGroup(Map<String, String> params);

    /**
     * 分页搜索校验任务表（用于下拉框远程搜索）
     *
     */
    Paging<Table> searchTables(Map<String, String> params);

    /**
     * 获取结果
     *
     */
    Object result(String id);

    /**
     * 刷新表列表
     *
     */
    String refreshTables(String id);

    /**
     * 刷新表字段
     *
     */
    String refreshFields(String id);

    /**
     * 获取所有任务列表（用于下拉选择）
     *
     * @return 任务VO列表
     */
    List<ValidateSyncTaskVO> getAll();

    /**
     * 分页查询校验结果明细
     *
     * @param params 包含 taskId、pageNum、pageSize；可选 lite=true 列表不查 CONTENT
     * @return 分页结果
     */
    Paging searchResult(Map<String, String> params);

    /**
     * 按明细主键查询单条（含完整 CONTENT）。
     *
     * @param id 明细 ID
     * @return 单条 Map，不存在返回 null
     */
    Object getValidateResultDetail(String id);

    String addTableGroup(Map<String, String> params);

    String editTableGroup(Map<String, String> params);

    String removeTableGroup(String id, String ids);
}
