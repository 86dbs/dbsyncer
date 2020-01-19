package org.dbsyncer.listener.mysql.net.impl;

import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;
import org.dbsyncer.listener.mysql.net.Transport;
import org.dbsyncer.listener.mysql.net.TransportContext;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractTransport implements Transport {
    protected Transport.Authenticator authenticator;
    protected final Context context = new Context();
    protected final AtomicBoolean verbose = new AtomicBoolean(true);

    public boolean isVerbose() {
        return this.verbose.get();
    }

    public void setVerbose(boolean verbose) {
        this.verbose.set(verbose);
    }

    public TransportContext getContext() {
        return context;
    }

    public Transport.Authenticator getAuthenticator() {
        return authenticator;
    }

    public void setAuthenticator(Transport.Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public static class Context implements TransportContext {
        //
        private long threadId;
        private String scramble;
        private int protocolVersion;
        private String serverHost;
        private int serverPort;
        private int serverStatus;
        private int serverCollation;
        private String serverVersion;
        private int serverCapabilities;

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("threadId", threadId)
                    .append("scramble", scramble)
                    .append("protocolVersion", protocolVersion)
                    .append("serverHost", serverHost)
                    .append("serverPort", serverPort)
                    .append("serverStatus", serverStatus)
                    .append("serverCollation", serverCollation)
                    .append("serverVersion", serverVersion)
                    .append("serverCapabilities", serverCapabilities).toString();
        }

        public long getThreadId() {
            return threadId;
        }

        public void setThreadId(long threadId) {
            this.threadId = threadId;
        }

        public String getScramble() {
            return scramble;
        }

        public void setScramble(String scramble) {
            this.scramble = scramble;
        }

        public int getProtocolVersion() {
            return protocolVersion;
        }

        public void setProtocolVersion(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public String getServerHost() {
            return serverHost;
        }

        public void setServerHost(String host) {
            this.serverHost = host;
        }

        public int getServerPort() {
            return serverPort;
        }

        public void setServerPort(int port) {
            this.serverPort = port;
        }

        public int getServerStatus() {
            return serverStatus;
        }

        public void setServerStatus(int serverStatus) {
            this.serverStatus = serverStatus;
        }

        public int getServerCollation() {
            return serverCollation;
        }

        public void setServerCollation(int serverCollation) {
            this.serverCollation = serverCollation;
        }

        public String getServerVersion() {
            return serverVersion;
        }

        public void setServerVersion(String serverVersion) {
            this.serverVersion = serverVersion;
        }

        public int getServerCapabilities() {
            return serverCapabilities;
        }

        public void setServerCapabilities(int serverCapabilities) {
            this.serverCapabilities = serverCapabilities;
        }
    }
}
