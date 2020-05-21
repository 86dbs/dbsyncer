package org.dbsyncer.manager.logger;

import org.dbsyncer.parser.enums.ErrorEnum;
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
        flushService.asyncWrite("0", msg);
    }

    @Override
    public void log(ErrorEnum error) {
        flushService.asyncWrite(error.getType(), error.getMessage());
    }

    @Override
    public void log(ErrorEnum error, String msg) {
        flushService.asyncWrite(error.getType(), msg);
    }
}
