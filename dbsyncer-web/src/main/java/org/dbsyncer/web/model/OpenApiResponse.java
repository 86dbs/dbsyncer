/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.model;

/**
 * OpenAPI统一响应格式
 *
 * @author 穿云
 * @version 1.0.0
 */
public class OpenApiResponse<T> {

    /**
     * 是否成功
     */
    private Boolean success;

    /**
     * 响应码
     */
    private Integer code;

    /**
     * 响应消息
     */
    private String message;

    /**
     * 响应数据
     */
    private T data;

    /**
     * 时间戳
     */
    private Long timestamp;

    public OpenApiResponse() {
        this.timestamp = System.currentTimeMillis();
    }

    public OpenApiResponse(Boolean success, Integer code, String message, T data) {
        this.success = success;
        this.code = code;
        this.message = message;
        this.data = data;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 成功响应
     */
    public static <T> OpenApiResponse<T> success(T data) {
        return new OpenApiResponse<>(true, 200, "success", data);
    }

    /**
     * 成功响应（带消息）
     */
    public static <T> OpenApiResponse<T> success(String message, T data) {
        return new OpenApiResponse<>(true, 200, message, data);
    }

    /**
     * 失败响应
     */
    public static <T> OpenApiResponse<T> fail(Integer code, String message) {
        return new OpenApiResponse<>(false, code, message, null);
    }

    /**
     * 失败响应（默认400错误码）
     */
    public static <T> OpenApiResponse<T> fail(String message) {
        return new OpenApiResponse<>(false, 400, message, null);
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
