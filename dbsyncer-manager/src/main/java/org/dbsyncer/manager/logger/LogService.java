package org.dbsyncer.manager.logger;

import org.dbsyncer.parser.enums.ErrorEnum;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-21 23:18
 */
public interface LogService {

    void log(String msg);

    void log(ErrorEnum errorEnum);

    void log(ErrorEnum errorEnum, String msg);
}
