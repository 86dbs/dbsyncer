package org.dbsyncer.parser.impl;

import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.flush.FlushService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-21 23:18
 */
@Component
public class LogServiceImpl implements LogService {

    @Resource
    private FlushService flushService;

    @Override
    public void log(LogType logType) {
        flushService.asyncWrite(logType.getType(), String.format("%s%s", logType.getName(), logType.getMessage()));
    }

    @Override
    public void log(LogType logType, String msg) {
        flushService.asyncWrite(logType.getType(), null == msg ? logType.getMessage() : msg);
    }

    @Override
    public void log(LogType logType, String format, Object... args) {
        flushService.asyncWrite(logType.getType(), String.format(format, args));
    }
}