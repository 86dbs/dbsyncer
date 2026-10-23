/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.notice;

import org.dbsyncer.sdk.model.NoticeContent;
import org.dbsyncer.sdk.model.NoticeStrategy;

import java.util.List;

/**
 * 消息通知决策引擎：按策略匹配、限流、合并后投递。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-06-16 12:00
 */
public interface NoticeDecisionEngine {

    /**
     * 接收业务事件并按策略路由。
     *
     * @param noticeContent 通知内容
     */
    void dispatch(NoticeContent noticeContent);

    /**
     * 用给定策略列表重建运行态（Registry / 周期调度）。
     * <p>策略数据由调用方从存储装载，引擎不依赖配置 Profile，避免启动期循环依赖。
     *
     * @param strategies 运行时策略；空列表表示清空
     */
    void reload(List<NoticeStrategy> strategies);
}
