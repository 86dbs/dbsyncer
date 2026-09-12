/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.model;

import org.dbsyncer.common.util.StringUtil;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-12 13:43
 */
public class HttpResult {

    private final int statusCode;
    private final String body;

    public HttpResult(int statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body == null ? StringUtil.EMPTY : body;
    }

    /**
     * @return HTTP 状态码是否为 200
     */
    public boolean isOk() {
        return statusCode == 200;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getBody() {
        return body;
    }
}
