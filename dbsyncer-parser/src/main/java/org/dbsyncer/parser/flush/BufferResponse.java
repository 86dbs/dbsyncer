package org.dbsyncer.parser.flush;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 18:11
 */
public interface BufferResponse {

    /**
     * 获取批处理数
     *
     * @return
     */
    int getTaskSize();

}