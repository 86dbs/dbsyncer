/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.parser.model.NoticeRule;

import java.util.List;
import java.util.Map;

/**
 * 告警通知规则。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface NoticeRuleService {

    /**
     * 全部规则。
     */
    List<NoticeRule> listAll();

    /**
     * 新增或更新。
     */
    String save(Map<String, String> params);

    /**
     * 按主键删除。
     */
    String remove(Map<String, String> params);
}
