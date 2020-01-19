package org.dbsyncer.listener.mysql.common.util;

import org.dbsyncer.listener.mysql.io.XInputStream;
import org.dbsyncer.listener.mysql.io.XOutputStream;

import java.net.Socket;

public final class IOUtils {

    public static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (Exception e) {
            // NOP
        }
    }

    public static void closeQuietly(XInputStream is) {
        try {
            is.close();
        } catch (Exception e) {
            // NOP
        }
    }

    public static void closeQuietly(XOutputStream os) {
        try {
            os.close();
        } catch (Exception e) {
            // NOP
        }
    }
}
