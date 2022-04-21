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

    @Override
    public String getOutputPlugin() {
        return MessageDecoderEnum.PG_OUTPUT.getType();
    }

    @Override
    public void withSlotOption(ChainedLogicalStreamBuilder builder) {
        builder.withSlotOption("proto_version", 1);
        builder.withSlotOption("publication_names", String.format("dbs_pub_%s_%s", config.getSchema(), config.getUsername()));
    }

}