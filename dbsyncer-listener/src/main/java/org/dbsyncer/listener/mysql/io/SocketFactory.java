package org.dbsyncer.listener.mysql.io;

import java.net.Socket;

public interface SocketFactory {

	Socket create(String host, int port) throws Exception;
}
