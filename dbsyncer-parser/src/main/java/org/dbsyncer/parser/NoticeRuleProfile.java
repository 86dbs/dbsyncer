/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.NoticeRule;

import java.util.List;

/**
 * 告警通知规则读写。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface NoticeRuleProfile {

    /**
     * 全部规则。
     */
    List<NoticeRule> listAll();

    /**
     * 按主键查询。
     */
    NoticeRule get(String id);

    /**
     * 按 (作用域类型, 作用域ID) 查一条。
     */
    NoticeRule getByScope(String scopeType, String scopeId);

    /**
     * 按作用域类型列出。
     */
    List<NoticeRule> listByScopeType(String scopeType);

    /**
     * 新增。
     */
    String add(NoticeRule rule);

    /**
     * 更新。
     */
    String update(NoticeRule rule);

    /**
     * 按主键删除。
     */
    void remove(String id);

    /**
     * 按作用域批量删除（分组/任务被删除时收尾）。
     */
    void removeByScopeId(String scopeType, String scopeId);
}
