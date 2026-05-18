/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.plugin.model.*;
import org.dbsyncer.sdk.model.NoticeContent;

import org.dbsyncer.sdk.enums.ModelEnum;

/**
 * WebHook通知服务实现基类
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/03/05 22:14
 */
public abstract class AbstractWebHookNoticeService extends AbstractNoticeService {

    protected String buildContent(NoticeContent noticeContent) {
        // 运行异常
        if (noticeContent instanceof MappingErrorContent) {
            MappingErrorContent meta = (MappingErrorContent) noticeContent;
            StringBuilder c = new StringBuilder();
            c.append(String.format("[%s] %s，任务数：%s\n",
                    getAppConfig().getName(), noticeContent.getTitle(), meta.getErrorItems().size()));
            c.append(String.format("%s\n\n", DateFormatUtil.now()));

            for (int i = 0; i < meta.getErrorItems().size(); i++) {
                MappingErrorContent.ErrorItem item = meta.getErrorItems().get(i);
                c.append(String.format("%d. %s(%s) 失败:%s, 成功:%s", i + 1, item.getName(), item.getModel().getName(), item.getFail(), item.getSuccess()));
                if (ModelEnum.FULL == item.getModel()) {
                    c.append(String.format("总数:%s", item.getTotal()));
                }
                c.append("\n");
            }
            return c.toString();
        }

        // 停止驱动
        if (noticeContent instanceof MappingStopContent) {
            MappingStopContent meta = (MappingStopContent) noticeContent;
            return String.format("[%s] %s %s(%s)", getAppConfig().getName(),
                    noticeContent.getTitle(), meta.getName(), meta.getModel().getName());
        }

        // 连接离线
        if (noticeContent instanceof ConnectorOfflineContent) {
            ConnectorOfflineContent meta = (ConnectorOfflineContent) noticeContent;
            StringBuilder c = new StringBuilder();
            c.append(String.format("[%s] %s，连接数：%s\n",
                    getAppConfig().getName(), noticeContent.getTitle(), meta.getErrorItems().size()));
            c.append(String.format("%s\n\n", DateFormatUtil.now()));

            for (int i = 0; i < meta.getErrorItems().size(); i++) {
                ConnectorOfflineContent.ErrorItem item = meta.getErrorItems().get(i);
                c.append(String.format("%d. %s(%s), %s", i + 1, item.getName(), item.getType(), item.getUrl()));
                c.append("\n");
            }
            return c.toString();
        }

        // 测试通知
        if (noticeContent instanceof TestNoticeContent) {
            TestNoticeContent meta = (TestNoticeContent) noticeContent;
            return String.format("[%s] %s %s", getAppConfig().getName(), noticeContent.getTitle(), meta.getContent());
        }
        return null;
    }
}
