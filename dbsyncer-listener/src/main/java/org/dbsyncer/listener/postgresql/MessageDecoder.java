package org.dbsyncer.listener.postgresql;

import org.dbsyncer.listener.event.RowChangedEvent;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.nio.ByteBuffer;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 22:59
 */
public interface MessageDecoder {

    default void postProcessBeforeInitialization(ConnectorFactory connectorFactory, DatabaseConnectorInstance connectorInstance) {
    }

    boolean skipMessage(ByteBuffer buffer, LogSequenceNumber startLsn, LogSequenceNumber lastReceiveLsn);

    RowChangedEvent processMessage(ByteBuffer buffer);

    String getSlotName();

    String getOutputPlugin();

    void withSlotOption(ChainedLogicalStreamBuilder builder);

    void setMetaId(String metaId);

    void setConfig(DatabaseConfig config);

}