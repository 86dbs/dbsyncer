package org.dbsyncer.listener.mysql.net.impl;

import org.dbsyncer.listener.mysql.io.impl.XInputStreamImpl;
import org.dbsyncer.listener.mysql.net.Packet;
import org.dbsyncer.listener.mysql.net.TransportInputStream;
import org.dbsyncer.listener.mysql.net.impl.packet.RawPacket;

import java.io.IOException;
import java.io.InputStream;

public class TransportInputStreamImpl extends XInputStreamImpl implements TransportInputStream {

    private int currentPacketSequence;

    public TransportInputStreamImpl(InputStream is) {
        super(is);
        this.readLimit = this.readCount = 0;
    }

    public TransportInputStreamImpl(InputStream is, int size) {
        super(is, size);
        this.readLimit = this.readCount = 0;
    }

    public Packet readPacket() throws IOException {
        final RawPacket r = readPacketHeader();

        final byte[] body = new byte[r.getLength()];

        this.read(body, 0, r.getLength());
        r.setPacketBody(body);

        return r;
    }

    private RawPacket readPacketHeader() throws IOException {
        final RawPacket r = new RawPacket();

        // read next header
        this.setReadLimit(4);
        int packetLength = this.readInt(3);
        this.currentPacketSequence = this.readInt(1); // consume packet sequence #

        this.setReadLimit(packetLength);
        r.setLength(packetLength);
        r.setSequence(this.currentPacketSequence);

        return r;
    }

    @Override
    public int read() throws IOException {
        if (this.readCount + 1 > this.readLimit) {
            readPacketHeader();
        }
        return super.read();
    }

    @Override
    public int read(final byte b[], int off, final int len) throws IOException {
        int left = len;

        // if we're about to read off the end of read-limit, see if this is a response
        // that spans multiple packets.
        while ((this.readCount + left) > this.readLimit) {

            // consume from middle of buffer to end of packet.
            int remaining_length = this.readLimit - this.readCount;
            super.read(b, off, remaining_length);

            readPacketHeader();

            left -= remaining_length;
            off += remaining_length;
        }

        // now consume whatever's left
        super.read(b, off, left);

        return len;
    }

    public int currentPacketLength() {
        return this.readLimit;
    }

    public int currentPacketSequence() {
        return this.currentPacketSequence;
    }
}
