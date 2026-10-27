package org.dbsyncer.parser;

import java.util.Map;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-21 23:18
 */
public interface LogService {

    void log(LogType logType);

    void log(LogType logType, String msg);

    void log(LogType logType, String format, Object... args);

    /**
     * 写入带操作人/IP/状态的日志（JSON 为摘要文案）。
     *
     * @param logType  日志类型
     * @param username 操作人/登录账号
     * @param ip       客户端 IP
     * @param status   状态码，见 {@link org.dbsyncer.common.enums.StatusEnum}
     * @param msg      摘要；空则用 logType.message
     */
    void log(LogType logType, String username, String ip, int status, String msg);

    /**
     * 写入带操作人/IP/状态的结构化日志（detail 序列化进 JSON）。
     *
     * @param logType  日志类型
     * @param username 操作人/登录账号
     * @param ip       客户端 IP
     * @param status   状态码，见 {@link org.dbsyncer.common.enums.StatusEnum}
     * @param detail   结构化内容（含 message / requestUri 等）
     */
    void log(LogType logType, String username, String ip, int status, Map<String, Object> detail);
}
