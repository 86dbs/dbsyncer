/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.NoticeMentionRule;

import java.util.List;

/**
 * 告警 @ 覆盖规则读写。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface NoticeMentionRuleProfile {

    /**
     * 全部规则。
     */
    List<NoticeMentionRule> listAll();

    /**
     * 按主键查询。
     */
    NoticeMentionRule get(String id);

    /**
     * 按 (作用域类型, 作用域ID) 查一条。
     */
    NoticeMentionRule getByScope(String scopeType, String scopeId);

    /**
     * 按作用域类型列出。
     */
    List<NoticeMentionRule> listByScopeType(String scopeType);

    /**
     * 新增。
     */
    String add(NoticeMentionRule rule);

    /**
     * 更新。
     */
    String update(NoticeMentionRule rule);

    /**
     * 按主键删除。
     */
    void remove(String id);
}
