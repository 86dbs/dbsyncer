package org.dbsyncer.connector.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public abstract class PingUtil {

    public static boolean ping(String ip, int port) {
        try {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(ip, port));
            s.close();
            return true;
        } catch (IOException e) {
        }
        return false;
    }
    
}
