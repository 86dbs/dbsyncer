/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.sdk.model.NoticeStrategyDef;

import java.util.List;
import java.util.Map;

/**
 * 告警策略业务。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface NoticeStrategyService {

    /**
     * 全部策略（DTO，供页面与运行时转换）。
     */
    List<NoticeStrategyDef> listAll();

    /**
     * 新增或更新一条策略，立即生效。
     *
     * @param params id（空则新增）、name、frequency、cron、enabled、noticeTypes（逗号分隔）
     * @return 策略 ID
     */
    String save(Map<String, String> params);

    /**
     * 按 ID 删除策略，立即生效。
     *
     * @param params 须含 id
     * @return 已删除的策略 ID
     */
    String remove(Map<String, String> params);
}
