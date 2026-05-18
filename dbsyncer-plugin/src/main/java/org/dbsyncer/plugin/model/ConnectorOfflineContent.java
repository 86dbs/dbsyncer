/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;


import org.dbsyncer.sdk.enums.NoticeTypeEnum;

import java.util.ArrayList;
import java.util.List;

/**
 * 连接离线信息
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-03-07 19:00
 */
public final class ConnectorOfflineContent extends org.dbsyncer.sdk.model.NoticeContent {

    private List<ErrorItem> errorItems;

    public ConnectorOfflineContent() {
        setNoticeType(NoticeTypeEnum.CONNECTOR_OFFLINE);
    }

    public void addErrorItem(ErrorItem item) {
        if (errorItems == null) {
            errorItems = new ArrayList<>();
        }
        errorItems.add(item);
    }

    public List<ErrorItem> getErrorItems() {
        return errorItems;
    }

    public static class ErrorItem {
        private String name;
        private String type;
        private String url;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }

}
