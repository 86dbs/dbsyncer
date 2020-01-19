package org.dbsyncer.listener.mysql.net.impl.packet;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.io.XInputStream;
import org.dbsyncer.listener.mysql.io.util.XDeserializer;
import org.dbsyncer.listener.mysql.io.util.XSerializer;
import org.dbsyncer.listener.mysql.net.Packet;

import java.io.IOException;

public class ErrorPacket extends AbstractPacket {
    private static final long serialVersionUID = -6842057808734657288L;

    public static final byte PACKET_MARKER = (byte) 0xFF;

    private int packetMarker;
    private int errorCode;
    private StringColumn slash;
    private StringColumn sqlState;
    private StringColumn errorMessage;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("packetMarker", packetMarker)
                .append("errorCode", errorCode)
                .append("slash", slash)
                .append("sqlState", sqlState)
                .append("errorMessage", errorMessage).toString();
    }

    public byte[] getPacketBody() {
        final XSerializer s = new XSerializer(64);
        s.writeInt(this.packetMarker, 1);
        s.writeInt(this.errorCode, 2);
        s.writeFixedLengthString(this.slash);
        s.writeFixedLengthString(this.sqlState);
        s.writeFixedLengthString(this.errorMessage);
        return s.toByteArray();
    }

    public int getPacketMarker() {
        return packetMarker;
    }

    public void setPacketMarker(int fieldCount) {
        this.packetMarker = fieldCount;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public StringColumn getSlash() {
        return slash;
    }

    public void setSlash(StringColumn slash) {
        this.slash = slash;
    }

    public StringColumn getSqlState() {
        return sqlState;
    }

    public void setSqlState(StringColumn sqlState) {
        this.sqlState = sqlState;
    }

    public StringColumn getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(StringColumn errorMessage) {
        this.errorMessage = errorMessage;
    }

    public static ErrorPacket valueOf(Packet packet) throws IOException {
        final XDeserializer d = new XDeserializer(packet.getPacketBody());
        final ErrorPacket r = new ErrorPacket();
        r.length = packet.getLength();
        r.sequence = packet.getSequence();
        r.packetMarker = d.readInt(1);
        r.errorCode = d.readInt(2);
        r.slash = d.readFixedLengthString(1);
        r.sqlState = d.readFixedLengthString(5);
        r.errorMessage = d.readFixedLengthString(d.available());
        return r;
    }

    public static ErrorPacket valueOf(int packetLength, int packetSequence, int packetMarker, XInputStream is)
            throws IOException {
        final ErrorPacket r = new ErrorPacket();
        r.length = packetLength;
        r.sequence = packetSequence;
        r.packetMarker = packetMarker;
        r.errorCode = is.readInt(2);
        r.slash = is.readFixedLengthString(1);
        r.sqlState = is.readFixedLengthString(5);
        r.errorMessage = is.readFixedLengthString(is.available());
        return r;
    }
}
