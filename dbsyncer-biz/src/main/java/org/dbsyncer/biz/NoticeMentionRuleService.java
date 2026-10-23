/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.parser.model.NoticeMentionRule;

import java.util.List;
import java.util.Map;

/**
 * 告警 @ 覆盖规则。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface NoticeMentionRuleService {

    /**
     * 全部规则。
     */
    List<NoticeMentionRule> listAll();

    /**
     * 新增或更新。
     */
    String save(Map<String, String> params);

    /**
     * 按主键删除。
     */
    String remove(Map<String, String> params);
}
