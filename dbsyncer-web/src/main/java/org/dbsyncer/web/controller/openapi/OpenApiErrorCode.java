/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

/**
 * OpenAPI 统一错误码常量
 *
 * @author 穿云
 * @version 2.0.0
 */
public final class OpenApiErrorCode {

    private OpenApiErrorCode() {
    }

    /** 请求参数错误（如参数缺失、格式错误、时间戳/Nonce 校验失败等） */
    public static final int BAD_REQUEST = 400;

    /** 未授权（如 Token 为空、无效、过期，或 secret 错误） */
    public static final int UNAUTHORIZED = 401;

    /** 禁止访问（如 IP 不在白名单） */
    public static final int FORBIDDEN = 403;

    /** 资源或能力未开放（如未开放 API） */
    public static final int NOT_FOUND = 404;

    /** 服务内部错误（如登录/刷新/处理异常） */
    public static final int INTERNAL_ERROR = 500;

    /** 服务暂不可用（如预加载未完成） */
    public static final int SERVICE_UNAVAILABLE = 503;
}
