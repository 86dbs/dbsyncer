package org.dbsyncer.parser.convert;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 22:55
 */
public interface Handler {

    /**
     * 值转换
     *
     * @param args 参数
     * @param value 值
     * @return
     */
    Object handle(String args, Object value);
}
