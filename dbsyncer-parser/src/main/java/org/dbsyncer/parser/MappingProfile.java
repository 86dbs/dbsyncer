/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.Mapping;

import java.util.List;

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

}
