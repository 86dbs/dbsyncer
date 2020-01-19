package org.dbsyncer.listener.mysql.net;

import org.dbsyncer.listener.mysql.io.XOutputStream;

import java.io.IOException;

public interface TransportOutputStream extends XOutputStream {

    void writePacket(Packet packet) throws IOException;

}
