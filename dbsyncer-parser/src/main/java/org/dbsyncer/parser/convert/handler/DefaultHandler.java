package org.dbsyncer.parser.convert.handler;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.parser.convert.Handler;

/**
 * 默认值
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:02
 */
public class DefaultHandler implements Handler {

    @Override
    public Object handle(String args, Object value) {
        return null == value || StringUtils.isBlank(String.valueOf(value)) ? args : value;
    }
}