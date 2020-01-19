package org.dbsyncer.listener.mysql.binlog.impl;

import org.dbsyncer.listener.mysql.binlog.BinlogEventParser;
import org.dbsyncer.listener.mysql.binlog.impl.event.BinlogEventV4HeaderImpl;
import org.dbsyncer.listener.mysql.binlog.impl.parser.FormatDescriptionEventParser;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.net.Transport;
import org.dbsyncer.listener.mysql.net.TransportInputStream;
import org.dbsyncer.listener.mysql.net.impl.EventInputStream;
import org.dbsyncer.listener.mysql.net.impl.packet.EOFPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.ErrorPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.OKPacket;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ReplicationBasedBinlogParser extends AbstractBinlogParser {
    private final boolean stopOnEOF;
    protected long heartbeatCount = 0;
    protected Long lastEventMillis = null;

    protected Transport transport;

    public ReplicationBasedBinlogParser(boolean stopOnEOF, String threadSuffixName) {
        this.stopOnEOF = stopOnEOF;
        super.threadSuffixName = threadSuffixName;
    }

    @Override
    protected void doStart() throws Exception {
        // NOP
    }

    @Override
    protected void doStop(long timeout, TimeUnit unit) throws Exception {
        // NOP
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    @Override
    public String getBinlogFileName() {
        return binlogFileName;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    private void readPacketMarker(TransportInputStream ts) throws IOException {
        final int packetMarker = ts.readInt(1);
        if (packetMarker != OKPacket.PACKET_MARKER) { // 0x00
            if ((byte) packetMarker == ErrorPacket.PACKET_MARKER) {
                final ErrorPacket packet = ErrorPacket.valueOf(ts.currentPacketLength(), ts.currentPacketSequence(), packetMarker, ts);
                throw new RuntimeException(packet.toString());
            } else if ((byte) packetMarker == EOFPacket.PACKET_MARKER) {
                if (stopOnEOF) {
                    throw new EOFException();
                } else {
                    final EOFPacket packet = EOFPacket.valueOf(ts.currentPacketLength(), ts.currentPacketSequence(), packetMarker, ts);
                    throw new IllegalArgumentException(packet.toString());
                }
            } else {
                throw new RuntimeException("assertion failed, invalid packet marker: " + packetMarker);
            }
        }
    }

    public long getHeartbeatCount() {
        return this.heartbeatCount;
    }

    public Long millisSinceLastEvent() {
        if (this.lastEventMillis == null)
            return null;

        return System.currentTimeMillis() - this.lastEventMillis;
    }

    @Override
    protected void doParse() throws Exception {
        final TransportInputStream is = this.transport.getInputStream();
        final EventInputStream es = new EventInputStream(is);

        final Context context = new Context(this);

        BinlogEventV4HeaderImpl header;
        while (isRunning()) {
            readPacketMarker(is);
            header = es.getNextBinlogHeader();

            boolean isFormatDescriptionEvent = header.getEventType() == MySQLConstants.FORMAT_DESCRIPTION_EVENT;

            if (header.getEventType() == MySQLConstants.HEARTBEAT_LOG_EVENT)
                this.heartbeatCount++;

            this.lastEventMillis = System.currentTimeMillis();

            // Parse the event body
            if (this.eventFilter != null && !this.eventFilter.accepts(header, context)) {
                /*
                 * FORMAT_DESCRIPTION events must always be parsed to ensure
                 * that we record checksum info -- if the caller has filtered
                 * them out, we still need to know.
                 */
                if (isFormatDescriptionEvent)
                    new FormatDescriptionEventParser().parse(es, header, context);
                else
                    this.defaultParser.parse(es, header, context);
            } else {
                BinlogEventParser parser = getEventParser(header.getEventType());
                if (parser == null)
                    parser = this.defaultParser;
                parser.parse(es, header, context);
            }

            if (isFormatDescriptionEvent)
                es.setChecksumEnabled(context.getChecksumEnabled());

            es.finishEvent(header);
        }
    }

}
