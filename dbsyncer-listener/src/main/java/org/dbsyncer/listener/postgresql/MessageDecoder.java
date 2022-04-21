package org.dbsyncer.listener.postgresql;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.nio.ByteBuffer;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 22:59
 */
public interface MessageDecoder {

    boolean skipMessage(ByteBuffer buffer, LogSequenceNumber lsn);

    RowChangedEvent processMessage(ByteBuffer buffer);

    String getSlotName();

    String getOutputPlugin();

    void withSlotOption(ChainedLogicalStreamBuilder builder);

    void setConfig(DatabaseConfig config);

}