package org.dbsyncer.parser.flush;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:34
 */
public interface BufferActuator {

    void offer(AbstractBufferTask task);

}