package org.dbsyncer.parser.flush;

import java.util.Queue;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:34
 */
public interface BufferActuator {

    Queue getBuffer();

    void offer(BufferRequest request);

}