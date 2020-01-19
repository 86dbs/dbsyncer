package org.dbsyncer.listener.mysql.net.impl.packet;

import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.util.XDeserializer;
import org.dbsyncer.listener.mysql.io.util.XSerializer;
import org.dbsyncer.listener.mysql.net.Packet;

import java.io.IOException;

public class OKPacket extends AbstractPacket {
    private static final long serialVersionUID = 3674181092305117701L;

    public static final byte PACKET_MARKER = (byte) 0x00;

    private int packetMarker;
    private UnsignedLong affectedRows;
    private UnsignedLong insertId;
    private int serverStatus;
    private int warningCount;
    private StringColumn message;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("packetMarker", packetMarker)
                .append("affectedRows", affectedRows)
                .append("insertId", insertId)
                .append("serverStatus", serverStatus)
                .append("warningCount", warningCount)
                .append("message", message).toString();
    }

    public byte[] getPacketBody() {
        final XSerializer s = new XSerializer(64);
        s.writeInt(this.packetMarker, 1);
        s.writeUnsignedLong(this.affectedRows);
        s.writeUnsignedLong(this.insertId);
        s.writeInt(this.serverStatus, 2);
        s.writeInt(this.warningCount, 2);
        if (this.message != null) s.writeFixedLengthString(this.message);
        return s.toByteArray();
    }

    public int getPacketMarker() {
        return packetMarker;
    }

    public void setPacketMarker(int packetMarker) {
        this.packetMarker = packetMarker;
    }

    public UnsignedLong getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(UnsignedLong affectedRows) {
        this.affectedRows = affectedRows;
    }

    public UnsignedLong getInsertId() {
        return insertId;
    }

    public void setInsertId(UnsignedLong insertId) {
        this.insertId = insertId;
    }

    public int getServerStatus() {
        return serverStatus;
    }

    public void setServerStatus(int serverStatus) {
        this.serverStatus = serverStatus;
    }

    public int getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    public StringColumn getMessage() {
        return message;
    }

    public void setMessage(StringColumn message) {
        this.message = message;
    }

    public static OKPacket valueOf(Packet packet) throws IOException {
        final XDeserializer d = new XDeserializer(packet.getPacketBody());
        final OKPacket r = new OKPacket();
        r.length = packet.getLength();
        r.sequence = packet.getSequence();
        r.packetMarker = d.readInt(1);
        r.affectedRows = d.readUnsignedLong();
        r.insertId = d.readUnsignedLong();
        r.serverStatus = d.readInt(2);
        r.warningCount = d.readInt(2);
        if (d.available() > 0) r.message = d.readFixedLengthString(d.available());
        return r;
    }
}
