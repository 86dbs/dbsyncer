package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

public final class BinlogEventV4HeaderImpl implements BinlogEventV4Header {
    private long timestamp;
    private int eventType;
    private long serverId;
    private long eventLength;
    private long nextPosition;
    private int flags;
    private long timestampOfReceipt;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("timestamp", timestamp)
                .append("eventType", eventType)
                .append("serverId", serverId)
                .append("eventLength", eventLength)
                .append("nextPosition", nextPosition)
                .append("flags", flags)
                .append("timestampOfReceipt", timestampOfReceipt).toString();
    }

    public int getHeaderLength() {
        return 19;
    }

    public long getPosition() {
        return this.nextPosition - this.eventLength;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public long getEventLength() {
        return eventLength;
    }

    public void setEventLength(long eventLength) {
        this.eventLength = eventLength;
    }

    public long getNextPosition() {
        return nextPosition;
    }

    public void setNextPosition(long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public long getTimestampOfReceipt() {
        return timestampOfReceipt;
    }

    public void setTimestampOfReceipt(long timestampOfReceipt) {
        this.timestampOfReceipt = timestampOfReceipt;
    }
}
