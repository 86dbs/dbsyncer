/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.notice;

import org.dbsyncer.sdk.model.NoticeContent;

/**
 * 消息通知决策引擎：按策略匹配、限流、合并后投递。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public interface NoticeDecisionEngine {

    /**
     * 接收业务事件并按内置策略路由。
     *
     * @param noticeContent 通知内容
     */
    void dispatch(NoticeContent noticeContent);
}
