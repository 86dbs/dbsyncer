package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

/**
 * Used to log an out of the ordinary event that occurred on the master.
 * It notifies the slave that something happened on the master that might
 * cause data to be in an inconsistent state.
 *
 * @ClassName: IncidentEvent
 * @author: AE86
 * @date: 2018年10月17日 下午2:26:26
 */
public final class IncidentEvent extends AbstractBinlogEventV4 {
    //
    public static final int EVENT_TYPE = MySQLConstants.INCIDENT_EVENT;

    //
    private int incidentNumber;
    private int messageLength;
    private StringColumn message;

    /**
     *
     */
    public IncidentEvent() {
    }

    public IncidentEvent(BinlogEventV4Header header) {
        this.header = header;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header)
                .append("incidentNumber", incidentNumber)
                .append("messageLength", messageLength)
                .append("message", message).toString();
    }

    /**
     *
     */
    public int getIncidentNumber() {
        return incidentNumber;
    }

    public void setIncidentNumber(int incidentNumber) {
        this.incidentNumber = incidentNumber;
    }

    public int getMessageLength() {
        return messageLength;
    }

    public void setMessageLength(int messageLength) {
        this.messageLength = messageLength;
    }

    public StringColumn getMessage() {
        return message;
    }

    public void setMessage(StringColumn message) {
        this.message = message;
    }
}
