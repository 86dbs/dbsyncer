package org.dbsyncer.parser.logger;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-21 23:18
 */
public interface LogService {

    void log(String msg);

    void log(LogType logType);

    void log(LogType logType, String msg);
}