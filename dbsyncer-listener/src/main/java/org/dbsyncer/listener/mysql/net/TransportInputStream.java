package org.dbsyncer.listener.mysql.net;

import org.dbsyncer.listener.mysql.io.XInputStream;

import java.io.IOException;

public interface TransportInputStream extends XInputStream {
    Packet readPacket() throws IOException;

    public int currentPacketLength();

    public int currentPacketSequence();
}
