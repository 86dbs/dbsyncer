package org.dbsyncer.listener.mysql.net.impl;

import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.MySQLUtils;
import org.dbsyncer.listener.mysql.io.util.XSerializer;
import org.dbsyncer.listener.mysql.net.Packet;
import org.dbsyncer.listener.mysql.net.Transport;
import org.dbsyncer.listener.mysql.net.TransportContext;
import org.dbsyncer.listener.mysql.net.TransportException;
import org.dbsyncer.listener.mysql.net.impl.packet.EOFPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.ErrorPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.OKPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.RawPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AuthenticatorImpl implements Transport.Authenticator {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticatorImpl.class);

    public static final int DEFAULT_CAPABILITIES = (MySQLConstants.CLIENT_LONG_FLAG | MySQLConstants.CLIENT_PROTOCOL_41 | MySQLConstants.CLIENT_SECURE_CONNECTION);

    protected String user;
    protected String password;
    protected String initialSchema;
    protected int clientCollation;
    protected int clientCapabilities;
    protected int maximumPacketLength;
    protected String encoding = "utf-8";

    public void login(Transport transport) throws IOException {
        final TransportContext ctx = transport.getContext();
        LOGGER.debug("start to login, user: {}, host: {}, port: {}", new Object[]{this.user, ctx.getServerHost(), ctx.getServerPort()});

        final XSerializer s = new XSerializer(64);
        s.writeInt(buildClientCapabilities(), 4);
        s.writeInt(this.maximumPacketLength, 4);
        s.writeInt(this.clientCollation > 0 ? this.clientCollation : ctx.getServerCollation(), 1);
        s.writeBytes((byte) 0, 23); // Fixed, all 0
        s.writeNullTerminatedString(StringColumn.valueOf(this.user.getBytes(this.encoding)));
        if (this.password == null) {
            s.writeBytes((byte) 0, 1);
        } else {
            s.writeInt(20, 1); // the length of the SHA1 encrypted password
            s.writeBytes(MySQLUtils.password41OrLater(this.password.getBytes(this.encoding), ctx.getScramble().getBytes(this.encoding)));
        }
        if (this.initialSchema != null)
            s.writeNullTerminatedString(StringColumn.valueOf(this.initialSchema.getBytes(this.encoding)));

        final RawPacket request = new RawPacket();
        request.setSequence(1);
        request.setPacketBody(s.toByteArray());
        request.setLength(request.getPacketBody().length);
        transport.getOutputStream().writePacket(request);
        transport.getOutputStream().flush();

        final Packet response = transport.getInputStream().readPacket();
        if (response.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
            final ErrorPacket error = ErrorPacket.valueOf(response);
            LOGGER.error("login failed, user: {}, error: {}", this.user, error);
            throw new TransportException(error);
        } else if (response.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
            LOGGER.error("Old style password authentication is not supported, upgrade user {} to a new style password or specify a different user", this.user);
            throw new RuntimeException("Old style password authentication not supported");
        } else if (response.getPacketBody()[0] == OKPacket.PACKET_MARKER) {
            final OKPacket ok = OKPacket.valueOf(response);
            LOGGER.debug("login successfully, user: {}, detail: {}", this.user, ok);
        } else {
            LOGGER.error("login failed, unknown packet: ", response);
            throw new RuntimeException("assertion failed, invalid packet: " + response);
        }
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getInitialSchema() {
        return initialSchema;
    }

    public void setInitialSchema(String schema) {
        this.initialSchema = schema;
    }

    public int getClientCollation() {
        return clientCollation;
    }

    public void setClientCollation(int collation) {
        this.clientCollation = collation;
    }

    public int getClientCapabilities() {
        return clientCapabilities;
    }

    public void setClientCapabilities(int capabilities) {
        this.clientCapabilities = capabilities;
    }

    public int getMaximumPacketLength() {
        return maximumPacketLength;
    }

    public void setMaximumPacketLength(int packetLength) {
        this.maximumPacketLength = packetLength;
    }

    protected int buildClientCapabilities() {
        int r = this.clientCapabilities > 0 ? this.clientCapabilities : DEFAULT_CAPABILITIES;
        if (this.initialSchema != null) r |= MySQLConstants.CLIENT_CONNECT_WITH_DB;
        return r;
    }
}
