package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

/**
 * 后面追加
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:04
 */
public class AppendHandler implements Handler {

    @Override
    public Object handle(String args, Object value) {
        if (null == value) {
            return args;
        }
        return new StringBuilder().append(value).append(args).toString();
    }
}