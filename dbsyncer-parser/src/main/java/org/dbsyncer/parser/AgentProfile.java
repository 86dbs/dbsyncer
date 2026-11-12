/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.AgentConfig;

import java.util.List;

/**
 * Agent 配置存储。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
public interface AgentProfile {

    /**
     * 按 ID 查询。
     *
     * @param id 主键
     * @return 配置；不存在则为 null
     */
    AgentConfig get(String id);

    /**
     * 列出全部。
     *
     * @return 配置列表
     */
    List<AgentConfig> listAll();

    /**
     * 新增。
     *
     * @param config 配置
     */
    void add(AgentConfig config);

    /**
     * 更新。
     *
     * @param config 配置
     */
    void edit(AgentConfig config);

    /**
     * 删除。
     *
     * @param id 主键
     */
    void remove(String id);
}
