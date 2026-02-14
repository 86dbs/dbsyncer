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
public class HttpResponse<T> {

    /**
     * 响应状态码
     */
    private int statusCode;

    /**
     * 响应内容
     */
    private String body;

    /**
     * 响应数据（反序列化后的对象）
     */
    private T data;

    /**
     * 是否成功
     */
    private boolean success;

    /**
     * 错误信息
     */
    private String message;

    /**
     * 响应时间（毫秒）
     */
    private long costTime;

    /**
     * 默认构造函数
     */
    public HttpResponse() {
    }

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
     * 创建成功的响应
     *
     * @param body 响应体
     * @param <T>  数据类型
     * @return 响应对象
     */
    public static <T> HttpResponse<T> success(String body) {
        HttpResponse<T> response = new HttpResponse<>();
        response.setStatusCode(200);
        response.setBody(body);
        response.setSuccess(true);
        return response;
    }

    /**
     * 创建成功的响应
     *
     * @param statusCode 状态码
     * @param body       响应体
     * @param <T>        数据类型
     * @return 响应对象
     */
    public static <T> HttpResponse<T> success(int statusCode, String body) {
        HttpResponse<T> response = new HttpResponse<>();
        response.setStatusCode(statusCode);
        response.setBody(body);
        response.setSuccess(statusCode >= 200 && statusCode < 300);
        return response;
    }

    /**
     * 创建失败的响应
     *
     * @param message 错误信息
     * @param <T>     数据类型
     * @return 响应对象
     */
    public static <T> HttpResponse<T> failure(String message) {
        HttpResponse<T> response = new HttpResponse<>();
        response.setSuccess(false);
        response.setMessage(message);
        return response;
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
     * 获取响应数据
     *
     * @return 响应数据
     */
    public T getData() {
        return data;
    }

    /**
     * 设置响应数据
     *
     * @param data 响应数据
     */
    public void setData(T data) {
        this.data = data;
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

    /**
     * 获取错误信息
     *
     * @return 错误信息
     */
    public String getMessage() {
        return message;
    }

    /**
     * 设置错误信息
     *
     * @param message 错误信息
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * 获取响应时间
     *
     * @return 响应时间（毫秒）
     */
    public long getCostTime() {
        return costTime;
    }

    /**
     * 设置响应时间
     *
     * @param costTime 响应时间（毫秒）
     */
    public void setCostTime(long costTime) {
        this.costTime = costTime;
    }

}
