package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:00
 */
public class PgOutputMessageDecoder extends AbstractMessageDecoder {

    private static final String PUBLICATION_NAMES = "pubNames";
    private static final String DEFAULT_PUBLICATION_NAME = "dbs_publication";

    @Override
    public String getOutputPlugin() {
        return MessageDecoderEnum.PG_OUTPUT.getType();
    }

    @Override
    public void withSlotOption(ChainedLogicalStreamBuilder builder) {
        builder.withSlotOption("proto_version", 1);
        String pubNames = config.getProperties().get(PUBLICATION_NAMES);
        builder.withSlotOption("publication_names", null != pubNames ? pubNames : DEFAULT_PUBLICATION_NAME);
    }

}