/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.model;

/**
 * HTTP 响应对象
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-10 00:01
 */
public final class HttpResponse {

    /**
     * 响应状态码
     */
    private int statusCode;

    /**
     * 响应内容
     */
    private String body;

    /**
     * 是否成功
     */
    private boolean success;

    /**
     * 构造函数
     *
     * @param statusCode 状态码
     * @param body       响应体
     */
    public HttpResponse(int statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body;
        this.success = (statusCode >= 200 && statusCode < 300);
    }

    /**
     * 获取状态码
     *
     * @return 状态码
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * 设置状态码
     *
     * @param statusCode 状态码
     */
    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    /**
     * 获取响应体
     *
     * @return 响应体
     */
    public String getBody() {
        return body;
    }

    /**
     * 设置响应体
     *
     * @param body 响应体
     */
    public void setBody(String body) {
        this.body = body;
    }

    /**
     * 是否成功
     *
     * @return 是否成功
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * 设置是否成功
     *
     * @param success 是否成功
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

}
