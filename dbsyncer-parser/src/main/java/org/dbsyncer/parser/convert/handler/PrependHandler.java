package org.dbsyncer.parser.convert.handler;

import org.dbsyncer.parser.convert.Handler;

/**
 * 前面追加
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:04
 */
public class PrependHandler implements Handler {

    @Override
    public Object handle(String args, Object value, java.util.Map<String, Object> row) {
        // row 参数未使用
        if (null == value) {
            return args;
        }
        return new StringBuilder().append(args).append(value).toString();
    }
}