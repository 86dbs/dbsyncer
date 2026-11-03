/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.NoticeStrategyConfig;

import java.util.List;

/**
 * 告警策略读写。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface NoticeStrategyProfile {

    /**
     * 全部策略。
     */
    List<NoticeStrategyConfig> listAll();

    /**
     * 按主键查询。
     */
    NoticeStrategyConfig get(String id);

    /**
     * 新增。
     */
    String add(NoticeStrategyConfig config);

    /**
     * 更新。
     */
    String update(NoticeStrategyConfig config);

    /**
     * 按主键删除。
     */
    void remove(String id);
}
