/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.constant;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-12 01:04
 */
public class HttpConstant {

    public static final String API = "api";

    public static final String METHOD = "method";

    public static final String CONTENT_TYPE = "contentType";

    public static final String PARAMS = "params";

    public static final String EXTRACT_PATH = "extractPath";

    public static final String EXTRACT_TOTAL = "extractTotal";

    public static final String WRITE_PATH = "writePath";

    /** 连接参数（连接器配置 properties） */
    // 连接超时（毫秒）
    public static final String CONNECTION_TIMEOUT_MS = "connection.timeout.ms";
    // 读取超时（毫秒）
    public static final String SOCKET_TIMEOUT_MS = "socket.timeout.ms";
    // 从连接池获取连接超时（毫秒）
    public static final String CONNECTION_REQUEST_TIMEOUT_MS = "connection.request.timeout.ms";
    // 重试次数
    public static final String RETRY_TIMES = "retry.times";
    // 字符编码
    public static final String CHARSET = "charset";

    /** 系统变量 */
    // 页数
    public static final String PAGE_INDEX = "$pageIndex$";
    // 页大小
    public static final String PAGE_SIZE = "$pageSize$";
    // 游标参数
    public static final String CURSOR = "$cursor$";
}
