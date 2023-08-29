package org.dbsyncer.parser.flush;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public interface BufferRequest {

    /**
     * 获取驱动ID
     *
     * @return
     */
    String getMetaId();
}