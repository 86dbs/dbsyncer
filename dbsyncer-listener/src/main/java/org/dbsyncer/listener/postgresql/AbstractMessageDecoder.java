package org.dbsyncer.listener.postgresql;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:04
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    protected DatabaseConfig config;

    @Override
    public boolean skipMessage(ByteBuffer buffer, LogSequenceNumber startLsn, LogSequenceNumber lastReceiveLsn) {
        return null == lastReceiveLsn || lastReceiveLsn.asLong() == 0 || startLsn.equals(lastReceiveLsn);
    }

    @Override
    public String getSlotName() {
        return String.format("dbs_slot_%s_%s", config.getSchema(), config.getUsername());
    }

    @Override
    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }
}