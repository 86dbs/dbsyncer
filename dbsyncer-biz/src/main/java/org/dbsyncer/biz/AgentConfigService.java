/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import java.util.List;
import java.util.Map;

/**
 * Agent 配置管理（开源）。启停由 {@code AgentService} 扩展点承担。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
public interface AgentConfigService {

    /**
     * 列出全部（不含私钥）。
     *
     * @return 视图列表
     */
    List<Map<String, Object>> list();

    /**
     * 按 ID 查询（不含私钥）。
     *
     * @param id 主键
     * @return 视图；不存在则为 null
     */
    Map<String, Object> get(String id);

    /**
     * 新增。
     *
     * @param params 表单
     * @return 主键
     */
    String add(Map<String, String> params);

    /**
     * 编辑。
     *
     * @param params 表单
     * @return 主键
     */
    String edit(Map<String, String> params);

    /**
     * 删除。运行中不可删。
     *
     * @param id 主键
     */
    void remove(String id);

    /**
     * 生成 AGENT 密钥。
     *
     * @return 密钥明文
     */
    String generateSecret();
}
