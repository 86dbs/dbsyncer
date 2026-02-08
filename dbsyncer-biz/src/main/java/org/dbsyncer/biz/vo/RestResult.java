/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.common.util.StringUtil;

import java.io.Serializable;

/**
 * 请求响应对象
 *
 * @author AE86
 * @version 1.0.0
 * @date 2017年3月30日 下午2:26:19
 */
public class RestResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否成功
     */
    private boolean success;

    /**
     * 数据
     */
    private Object data;

    /**
     * 消息
     */
    private String message;

    /**
     * 状态码
     */
    private int status;

    public RestResult(boolean success, Object data, String message, int status) {
        this.success = success;
        this.data = data;
        this.message = message;
        this.status = status;
    }

    public static RestResult restFail(String message) {
        return new RestResult(false, null, message, 500);
    }

    public static RestResult restFail(String message, int status) {
        return new RestResult(false, null, message, status);
    }

    public static RestResult restSuccess(Object data) {
        return new RestResult(true, data, StringUtil.EMPTY, 200);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "RestResult [success=" + success + ", data=" + data + ", message=" + message + ", status=" + status + "]";
    }
}
