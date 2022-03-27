package org.dbsyncer.parser.flush;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 18:11
 */
public abstract class AbstractFlushTask {

    /**
     * 获取批处理数
     *
     * @return
     */
    public abstract int getFlushTaskSize();

}