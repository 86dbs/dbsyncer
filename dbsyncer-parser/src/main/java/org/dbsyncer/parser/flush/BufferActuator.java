package org.dbsyncer.parser.flush;

import org.dbsyncer.parser.flush.model.AbstractRequest;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 17:34
 */
public interface BufferActuator {

    void offer(AbstractRequest task);

}