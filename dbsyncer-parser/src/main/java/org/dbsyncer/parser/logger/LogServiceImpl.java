package org.dbsyncer.parser.logger;

import org.dbsyncer.parser.flush.FlushService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-21 23:18
 */
@Component
public class LogServiceImpl implements LogService {

    @Autowired
    private FlushService flushService;

    @Override
    public void log(String msg) {
        flushService.asyncWrite(LogType.SystemLog.INFO.getType(), msg);
    }

    @Override
    public void log(LogType logType) {
        flushService.asyncWrite(logType.getType(), logType.getMessage());
    }

    @Override
    public void log(LogType logType, String format, Object... args) {
        flushService.asyncWrite(logType.getType(), String.format(format, args));
    }
}