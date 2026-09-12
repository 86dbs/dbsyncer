/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.impl;

import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ContentType;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.model.HttpResult;
import org.dbsyncer.common.util.HttpClientUtil;
import org.dbsyncer.sdk.notice.NoticeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractNoticeService implements NoticeService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private AppConfig appConfig;

    public AppConfig getAppConfig() {
        return appConfig;
    }

    public void setAppConfig(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    protected void send(String url, String message) {
        try {
            HttpResult result = HttpClientUtil.post(url, message, ContentType.APPLICATION_JSON);
            if (!result.isOk()) {
                throw new IllegalArgumentException(result.getBody());
            }
            logger.info("Send message:{}, result:{}", message, result.getBody());
        } catch (HttpHostConnectException e) {
            throw new IllegalArgumentException("网络连接异常，无法连接");
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            if (e.getCause() instanceof HttpHostConnectException) {
                throw new IllegalArgumentException("网络连接异常，无法连接");
            }
            throw new RuntimeException(e);
        }
    }
}
