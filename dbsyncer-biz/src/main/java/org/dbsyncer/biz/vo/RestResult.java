/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

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

    public RestResult(boolean success, Object data, int status) {
        this.success = success;
        this.data = data;
        this.status = status;
    }

    public static RestResult restFail(Object msg) {
        return new RestResult(false, msg, 500);
    }

    public static RestResult restFail(Object msg, int status) {
        return new RestResult(false, msg, status);
    }

    public static RestResult restSuccess(Object data) {
        return new RestResult(true, data, 200);
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
