package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:00
 */
public class TestDecodingMessageDecoder extends AbstractMessageDecoder {

    @Override
    public String getOutputPlugin() {
        return MessageDecoderEnum.TEST_DECODING.getType();
    }

    @Override
    public void withSlotOption(ChainedLogicalStreamBuilder builder) {
        builder.withSlotOption("include-xids", true)
                .withSlotOption("skip-empty-xacts", true);
    }

}
