/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.DatabaseSyncTaskVO;
import org.dbsyncer.common.model.Paging;

import java.util.Map;

/**
 * 整库迁移业务服务
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 00:00
 */
public interface DatabaseSyncService {

    /**
     * 获取任务详情
     *
     * @param id 任务 ID
     * @return 任务 VO
     */
    DatabaseSyncTaskVO get(String id);

    /**
     * 新增任务
     *
     * @param params 任务参数
     * @return 任务 ID
     */
    String add(Map<String, String> params);

    /**
     * 修改任务
     *
     * @param params 任务参数（含 id）
     * @return 任务 ID
     */
    String edit(Map<String, String> params);

    /**
     * 删除任务
     *
     * @param id 任务 ID
     * @return 操作结果提示
     */
    String delete(String id);

    /**
     * 启动任务
     *
     * @param id 任务 ID
     * @return 操作结果提示
     */
    String start(String id);

    /**
     * 停止任务
     *
     * @param id 任务 ID
     * @return 操作结果提示
     */
    String stop(String id);

    /**
     * 分页搜索任务列表
     *
     * @param params 查询参数（pageNum、pageSize、searchKey）
     * @return 分页结果
     */
    Paging<DatabaseSyncTaskVO> search(Map<String, String> params);

    /**
     * 预览连接器下表列表（滚动分页）
     *
     * @param params connectorId、database、schema、searchKey、offset、limit
     * @return 分页数据及类型统计
     */
    Map<String, Object> previewTables(Map<String, String> params);
}
