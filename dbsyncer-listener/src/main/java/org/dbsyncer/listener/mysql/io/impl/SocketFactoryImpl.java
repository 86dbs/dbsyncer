package org.dbsyncer.listener.mysql.io.impl;

import org.dbsyncer.listener.mysql.io.SocketFactory;

import java.net.Socket;

public class SocketFactoryImpl implements SocketFactory {
    private boolean keepAlive = false;
    private boolean tcpNoDelay = false;
    private int receiveBufferSize = -1;

    public Socket create(String host, int port) throws Exception {
        final Socket r = new Socket(host, port);
        r.setKeepAlive(this.keepAlive);
        r.setTcpNoDelay(this.tcpNoDelay);
        if (this.receiveBufferSize > 0) r.setReceiveBufferSize(this.receiveBufferSize);
        return r;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }
}
