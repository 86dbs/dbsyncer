package org.dbsyncer.listener.mysql.net.impl.packet;

import org.dbsyncer.listener.mysql.net.Packet;

public abstract class AbstractPacket implements Packet {
    private static final long serialVersionUID = -2762990065527029085L;

    protected int length;
    protected int sequence;

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }
}
