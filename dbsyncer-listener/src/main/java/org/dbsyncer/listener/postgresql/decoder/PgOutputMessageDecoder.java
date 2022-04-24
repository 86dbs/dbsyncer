package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.listener.postgresql.enums.MessageTypeEnum;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:00
 */
public class PgOutputMessageDecoder extends AbstractMessageDecoder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean skipMessage(ByteBuffer buffer, LogSequenceNumber startLsn, LogSequenceNumber lastReceiveLsn) {
        if (super.skipMessage(buffer, startLsn, lastReceiveLsn)) {
            return true;
        }
        int position = buffer.position();
        try {
            MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
            switch (type) {
                case BEGIN:
                case COMMIT:
                case RELATION:
                case TRUNCATE:
                case TYPE:
                case ORIGIN:
                case NONE:
                    return true;
                default:
                    // TABLE|INSERT|UPDATE|DELETE
                    return false;
            }
        } finally {
            buffer.position(position);
        }
    }

    @Override
    public RowChangedEvent processMessage(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
        }
        MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
        if (MessageTypeEnum.TABLE == type) {
            int offset = buffer.arrayOffset();
            byte[] source = buffer.array();
            return parseMessage(new String(source, offset, (source.length - offset)));
        }
        return null;
    }

    @Override
    public String getOutputPlugin() {
        return MessageDecoderEnum.PG_OUTPUT.getType();
    }

    @Override
    public void withSlotOption(ChainedLogicalStreamBuilder builder) {
        builder.withSlotOption("proto_version", 1);
        builder.withSlotOption("publication_names", String.format("dbs_pub_%s_%s", config.getSchema(), config.getUsername()));
    }

    private RowChangedEvent parseMessage(String message) {
        logger.info(message);

        return null;
    }

}