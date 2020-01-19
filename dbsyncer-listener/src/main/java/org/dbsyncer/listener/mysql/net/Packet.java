package org.dbsyncer.listener.mysql.net;

import java.io.IOException;
import java.io.Serializable;

public interface Packet extends Serializable {

	int getLength();

	int getSequence();

	byte[] getPacketBody() throws IOException;
}
