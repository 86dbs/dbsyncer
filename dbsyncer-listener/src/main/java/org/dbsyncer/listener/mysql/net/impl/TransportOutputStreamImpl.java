package org.dbsyncer.listener.mysql.net.impl;

import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.io.impl.XOutputStreamImpl;
import org.dbsyncer.listener.mysql.net.Packet;
import org.dbsyncer.listener.mysql.net.TransportOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class TransportOutputStreamImpl extends XOutputStreamImpl implements TransportOutputStream {

    public TransportOutputStreamImpl(OutputStream out) {
        super(out);
    }

    public void writePacket(Packet packet) throws IOException {
        //
        final byte body[] = packet.getPacketBody();
        if (body.length < MySQLConstants.MAX_PACKET_LENGTH) { // Single packet
            writeInt(body.length, 3);
            writeInt(packet.getSequence(), 1);
            writeBytes(body);
            return;
        }

        // If the length of the packet is greater than the value of MAX_PACKET_LENGTH,
        // which is defined to be power(2, 24) �C 1 in sql/net_serv.cc, the packet gets
        // split into smaller packets with bodies of MAX_PACKET_LENGTH plus the last
        // packet with a body that is shorter than MAX_PACKET_LENGTH.
        int offset = 0;
        int sequence = packet.getSequence();
        for (; offset + MySQLConstants.MAX_PACKET_LENGTH <= body.length; offset += MySQLConstants.MAX_PACKET_LENGTH) {
            writeInt(MySQLConstants.MAX_PACKET_LENGTH, 3);
            writeInt(sequence++, 1);
            writeBytes(body, offset, MySQLConstants.MAX_PACKET_LENGTH);
        }

        // The last short packet will always be present even if it must have a zero-length body.
        // It serves as an indicator that there are no more packet parts left in the stream for this large packet.
        writeInt(body.length - offset, 3);
        writeInt(sequence++, 1);
        writeBytes(body, offset, body.length - offset);
    }
}
