package org.dbsyncer.listener.postgresql;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 22:59
 */
public interface MessageDecoder {

    String getSlotName();

    String getOutputPlugin();

    void withSlotOption(ChainedLogicalStreamBuilder builder);

    void setMessageDecoderContext(MessageDecoderContext messageDecoderContext);

}
