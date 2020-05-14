package org.dbsyncer.listener.config;

public class Host {
    private String ip;
    private int    port;

    public Host(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}