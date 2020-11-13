package org.dbsyncer.listener.mysql;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.*;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.*;
import com.github.shyiko.mysql.binlog.network.protocol.*;
import com.github.shyiko.mysql.binlog.network.protocol.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BinaryLogRemoteClient implements BinaryLogClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final SSLSocketFactory DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory() {

        @Override
        protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
            sc.init(null, new TrustManager[] {
                    new X509TrustManager() {

                        @Override
                        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) { }

                        @Override
                        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) { }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            }, null);
        }
    };

    // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
    private static final int MAX_PACKET_LENGTH = 16777215;

    private final String  hostname;
    private final int     port;
    private final String  schema;
    private final String  username;
    private final String  password;
    private       SSLMode sslMode = SSLMode.DISABLED;

    private          boolean blocking       = true;
    private          long    serverId       = 65535;
    private volatile String  binlogFilename;
    private volatile long    binlogPosition = 4;
    private volatile long    connectionId;

    private final Object  gtidSetAccessLock = new Object();
    private       GtidSet gtidSet;
    private       boolean gtidSetFallbackToPurged;
    private       boolean useBinlogFilenamePositionInGtidMode;
    private       String  gtid;
    private       boolean tx;

    private       EventDeserializer                             eventDeserializer  = new EventDeserializer();
    private final List<BinaryLogRemoteClient.EventListener>     eventListeners     = new CopyOnWriteArrayList<>();
    private final List<BinaryLogRemoteClient.LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<>();

    private volatile PacketChannel channel;
    private volatile boolean       connected;

    private int timeout = 3000;

    private final Lock connectLock = new ReentrantLock();

    /**
     * Alias for BinaryLogRemoteClient(hostname, port, &lt;no schema&gt; = null, username, password).
     *
     * @see BinaryLogRemoteClient#BinaryLogRemoteClient(String, int, String, String, String)
     */
    public BinaryLogRemoteClient(String hostname, int port, String username, String password) {
        this(hostname, port, null, username, password);
    }

    /**
     * @param hostname mysql server hostname
     * @param port     mysql server port
     * @param schema   database name, nullable. Note that this parameter has nothing to do with event filtering. It's used only during the
     *                 authentication.
     * @param username login name
     * @param password password
     */
    public BinaryLogRemoteClient(String hostname, int port, String schema, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.schema = schema;
        this.username = username;
        this.password = password;
    }

    @Override
    public void connect() throws Exception {
        try {
            connectLock.lock();
            if (isConnected()) {
                throw new IllegalStateException("BinaryLogRemoteClient is already connected");
            }
            openChannel();
            // dump binary log
            requestBinaryLogStream(channel);
            notifyConnectEvent();
            ensureEventDeserializerHasRequiredEDDs();
            listenForEventPackets(channel);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void disconnect() throws Exception {
        if (!connected) {
            try {
                connectLock.lock();
                closeChannel(channel);
                notifyDisconnectEvent();
            } finally {
                connectLock.unlock();
            }
        }
    }

    @Override
    public boolean isConnected() {
        return this.connected;
    }

    @Override
    public void registerEventListener(BinaryLogRemoteClient.EventListener eventListener) {
        eventListeners.add(eventListener);
    }

    @Override
    public void registerLifecycleListener(BinaryLogRemoteClient.LifecycleListener lifecycleListener) {
        lifecycleListeners.add(lifecycleListener);
    }

    private void openChannel() throws IOException {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(hostname, port), timeout);
            channel = new PacketChannel(socket);
            if (channel.getInputStream().peek() == -1) {
                throw new EOFException();
            }
        } catch (IOException e) {
            closeChannel(channel);
            throw new IOException("Failed to connect to MySQL on " + hostname + ":" + port + ". Please make sure it's running.", e);
        }
        GreetingPacket greetingPacket = receiveGreeting(channel);
        authenticate(channel, greetingPacket);
        connectionId = greetingPacket.getThreadId();
        if ("".equals(binlogFilename)) {
            synchronized (gtidSetAccessLock) {
                if (gtidSet != null && "".equals(gtidSet.toString()) && gtidSetFallbackToPurged) {
                    gtidSet = new GtidSet(fetchGtidPurged(channel));
                }
            }
        }
        if (binlogFilename == null) {
            fetchBinlogFilenameAndPosition(channel);
        }
        if (binlogPosition < 4) {
            logger.warn("Binary log position adjusted from {} to {}", binlogPosition, 4);
            binlogPosition = 4;
        }
        ChecksumType checksumType = fetchBinlogChecksum(channel);
        if (checksumType != ChecksumType.NONE) {
            confirmSupportOfChecksum(channel, checksumType);
        }
        synchronized (gtidSetAccessLock) {
            String position = gtidSet != null ? gtidSet.toString() : binlogFilename + "/" + binlogPosition;
            logger.info("Connected to {}:{} at {} ({}cid:{})", hostname, port, position, (blocking ? "sid:" + serverId + ", " : ""),
                    connectionId);
        }
        connected = true;
    }

    private void listenForEventPackets(final PacketChannel channel) {
        ByteArrayInputStream inputStream = channel.getInputStream();
        try {
            while (inputStream.peek() != -1) {
                int packetLength = inputStream.readInteger(3);
                //noinspection ResultOfMethodCallIgnored
                // 1 byte for sequence
                inputStream.skip(1);
                int marker = inputStream.read();
                // 255 error
                if (marker == 0xFF) {
                    ErrorPacket errorPacket = new ErrorPacket(inputStream.read(packetLength - 1));
                    throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                            errorPacket.getSqlState());
                }
                // 254 empty
                if (marker == 0xFE && !blocking) {
                    break;
                }
                Event event;
                try {
                    event = eventDeserializer.nextEvent(packetLength == MAX_PACKET_LENGTH ?
                            new ByteArrayInputStream(readPacketSplitInChunks(inputStream, packetLength - 1)) :
                            inputStream);
                    if (event == null) {
                        throw new EOFException();
                    }
                } catch (Exception e) {
                    Throwable cause = e instanceof EventDataDeserializationException ? e.getCause() : e;
                    if (cause instanceof EOFException || cause instanceof SocketException) {
                        throw e;
                    }
                    if (connected) {
                        for (BinaryLogRemoteClient.LifecycleListener lifecycleListener : lifecycleListeners) {
                            lifecycleListener.onEventDeserializationFailure(this, e);
                        }
                    }
                    continue;
                }
                if (connected) {
                    updateGtidSet(event);
                    notifyEventListeners(event);
                    updateClientBinlogFilenameAndPosition(event);
                }
            }
        } catch (Exception e) {
            if (connected) {
                for (BinaryLogRemoteClient.LifecycleListener lifecycleListener : lifecycleListeners) {
                    lifecycleListener.onCommunicationFailure(this, e);
                }
            }
        }
    }

    private void ensureEventDeserializerHasRequiredEDDs() {
        ensureEventDataDeserializerIfPresent(EventType.ROTATE, RotateEventDataDeserializer.class);
        synchronized (gtidSetAccessLock) {
            if (gtidSet != null) {
                ensureEventDataDeserializerIfPresent(EventType.GTID, GtidEventDataDeserializer.class);
                ensureEventDataDeserializerIfPresent(EventType.QUERY, QueryEventDataDeserializer.class);
            }
        }
    }

    private GreetingPacket receiveGreeting(final PacketChannel channel) throws IOException {
        byte[] initialHandshakePacket = channel.read();
        /* error */
        if (initialHandshakePacket[0] == (byte) 0xFF) {
            byte[] bytes = Arrays.copyOfRange(initialHandshakePacket, 1, initialHandshakePacket.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
        }
        return new GreetingPacket(initialHandshakePacket);
    }

    private void requestBinaryLogStream(final PacketChannel channel) throws IOException {
        // http://bugs.mysql.com/bug.php?id=71178
        long serverId = blocking ? this.serverId : 0;
        Command dumpBinaryLogCommand;
        synchronized (gtidSetAccessLock) {
            if (gtidSet != null) {
                dumpBinaryLogCommand = new DumpBinaryLogGtidCommand(serverId,
                        useBinlogFilenamePositionInGtidMode ? binlogFilename : "",
                        useBinlogFilenamePositionInGtidMode ? binlogPosition : 4,
                        gtidSet);
            } else {
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, binlogFilename, binlogPosition);
            }
        }
        channel.write(dumpBinaryLogCommand);
    }

    private void ensureEventDataDeserializerIfPresent(EventType eventType,
                                                      Class<? extends EventDataDeserializer<?>> eventDataDeserializerClass) {
        EventDataDeserializer<?> eventDataDeserializer = eventDeserializer.getEventDataDeserializer(eventType);
        if (eventDataDeserializer.getClass() != eventDataDeserializerClass &&
                eventDataDeserializer.getClass() != EventDeserializer.EventDataWrapper.Deserializer.class) {
            EventDataDeserializer<?> internalEventDataDeserializer;
            try {
                internalEventDataDeserializer = eventDataDeserializerClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            eventDeserializer.setEventDataDeserializer(eventType,
                    new EventDeserializer.EventDataWrapper.Deserializer(internalEventDataDeserializer,
                            eventDataDeserializer));
        }
    }

    private void authenticate(final PacketChannel channel, GreetingPacket greetingPacket) throws IOException {
        int collation = greetingPacket.getServerCollation();
        int packetNumber = 1;

        boolean usingSSLSocket = false;
        if (sslMode != SSLMode.DISABLED) {
            boolean serverSupportsSSL = (greetingPacket.getServerCapabilities() & ClientCapabilities.SSL) != 0;
            if (!serverSupportsSSL && (sslMode == SSLMode.REQUIRED || sslMode == SSLMode.VERIFY_CA ||
                    sslMode == SSLMode.VERIFY_IDENTITY)) {
                throw new IOException("MySQL server does not support SSL");
            }
            if (serverSupportsSSL) {
                SSLRequestCommand sslRequestCommand = new SSLRequestCommand();
                sslRequestCommand.setCollation(collation);
                channel.write(sslRequestCommand, packetNumber++);
                SSLSocketFactory sslSocketFactory = sslMode == SSLMode.REQUIRED || sslMode == SSLMode.PREFERRED ?
                        DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY :
                        new DefaultSSLSocketFactory();

                channel.upgradeToSSL(sslSocketFactory, sslMode == SSLMode.VERIFY_IDENTITY ? new TLSHostnameVerifier() : null);
                usingSSLSocket = true;
            }
        }
        AuthenticateCommand authenticateCommand = new AuthenticateCommand(schema, username, password,
                greetingPacket.getScramble());
        authenticateCommand.setCollation(collation);
        channel.write(authenticateCommand, packetNumber);
        byte[] authenticationResult = channel.read();
        /* ok */
        if (authenticationResult[0] != (byte) 0x00) {
            /* error */
            if (authenticationResult[0] == (byte) 0xFF) {
                byte[] bytes = Arrays.copyOfRange(authenticationResult, 1, authenticationResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                        errorPacket.getSqlState());
            } else if (authenticationResult[0] == (byte) 0xFE) {
                switchAuthentication(channel, authenticationResult, usingSSLSocket);
            } else {
                throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + ")");
            }
        }
    }

    private void switchAuthentication(final PacketChannel channel, byte[] authenticationResult, boolean usingSSLSocket)
            throws IOException {
        /*
            Azure-MySQL likes to tell us to switch authentication methods, even though
            we haven't advertised that we support any.  It uses this for some-odd
            reason to send the real password scramble.
        */
        ByteArrayInputStream buffer = new ByteArrayInputStream(authenticationResult);
        //noinspection ResultOfMethodCallIgnored
        buffer.read(1);

        String authName = buffer.readZeroTerminatedString();
        if ("mysql_native_password".equals(authName)) {
            String scramble = buffer.readZeroTerminatedString();

            Command switchCommand = new AuthenticateNativePasswordCommand(scramble, password);
            channel.write(switchCommand, (usingSSLSocket ? 4 : 3));
            byte[] authResult = channel.read();

            if (authResult[0] != (byte) 0x00) {
                byte[] bytes = Arrays.copyOfRange(authResult, 1, authResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                        errorPacket.getSqlState());
            }
        } else {
            throw new AuthenticationException("Unsupported authentication type: " + authName);
        }
    }

    private String fetchGtidPurged(final PacketChannel channel) throws IOException {
        channel.write(new QueryCommand("show global variables like 'gtid_purged'"));
        ResultSetRowPacket[] resultSet = readResultSet(channel);
        if (resultSet.length != 0) {
            return resultSet[0].getValue(1).toUpperCase();
        }
        return "";
    }

    private void fetchBinlogFilenameAndPosition(final PacketChannel channel) throws IOException {
        channel.write(new QueryCommand("show master status"));
        ResultSetRowPacket[] resultSet = readResultSet(channel);
        if (resultSet.length == 0) {
            throw new IOException("Failed to determine binlog filename/position");
        }
        ResultSetRowPacket resultSetRow = resultSet[0];
        binlogFilename = resultSetRow.getValue(0);
        binlogPosition = Long.parseLong(resultSetRow.getValue(1));
    }

    private ChecksumType fetchBinlogChecksum(final PacketChannel channel) throws IOException {
        channel.write(new QueryCommand("show global variables like 'binlog_checksum'"));
        ResultSetRowPacket[] resultSet = readResultSet(channel);
        if (resultSet.length == 0) {
            return ChecksumType.NONE;
        }
        return ChecksumType.valueOf(resultSet[0].getValue(1).toUpperCase());
    }

    private void confirmSupportOfChecksum(final PacketChannel channel, ChecksumType checksumType) throws IOException {
        channel.write(new QueryCommand("set @master_binlog_checksum= @@global.binlog_checksum"));
        byte[] statementResult = channel.read();
        /* error */
        if (statementResult[0] == (byte) 0xFF) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
        }
        eventDeserializer.setChecksumType(checksumType);
    }

    private byte[] readPacketSplitInChunks(ByteArrayInputStream inputStream, int packetLength) throws IOException {
        byte[] result = inputStream.read(packetLength);
        int chunkLength;
        do {
            chunkLength = inputStream.readInteger(3);
            //noinspection ResultOfMethodCallIgnored
            inputStream.skip(1); // 1 byte for sequence
            result = Arrays.copyOf(result, result.length + chunkLength);
            inputStream.fill(result, result.length - chunkLength, chunkLength);
        } while (chunkLength == Packet.MAX_LENGTH);
        return result;
    }

    private void updateClientBinlogFilenameAndPosition(Event event) {
        EventHeader eventHeader = event.getHeader();
        EventType eventType = eventHeader.getEventType();
        if (eventType == EventType.ROTATE) {
            RotateEventData rotateEventData = (RotateEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
            binlogFilename = rotateEventData.getBinlogFilename();
            binlogPosition = rotateEventData.getBinlogPosition();
        } else
            // do not update binlogPosition on TABLE_MAP so that in case of reconnect (using a different instance of
            // client) table mapping cache could be reconstructed before hitting row mutation event
            if (eventType != EventType.TABLE_MAP && eventHeader instanceof EventHeaderV4) {
                EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
                long nextBinlogPosition = trackableEventHeader.getNextPosition();
                if (nextBinlogPosition > 0) {
                    binlogPosition = nextBinlogPosition;
                }
            }
    }

    private void updateGtidSet(Event event) {
        synchronized (gtidSetAccessLock) {
            if (gtidSet == null) {
                return;
            }
        }
        EventHeader eventHeader = event.getHeader();
        switch (eventHeader.getEventType()) {
            case GTID:
                GtidEventData gtidEventData = (GtidEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                gtid = gtidEventData.getGtid();
                break;
            case XID:
                commitGtid();
                tx = false;
                break;
            case QUERY:
                QueryEventData queryEventData = (QueryEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                String sql = queryEventData.getSql();
                if (sql == null) {
                    break;
                }
                if ("BEGIN".equals(sql)) {
                    tx = true;
                } else if ("COMMIT".equals(sql) || "ROLLBACK".equals(sql)) {
                    commitGtid();
                    tx = false;
                } else if (!tx) {
                    // auto-commit query, likely DDL
                    commitGtid();
                }
            default:
        }
    }

    private void commitGtid() {
        if (gtid != null) {
            synchronized (gtidSetAccessLock) {
                gtidSet.add(gtid);
            }
        }
    }

    private ResultSetRowPacket[] readResultSet(final PacketChannel channel) throws IOException {
        List<ResultSetRowPacket> resultSet = new LinkedList<ResultSetRowPacket>();
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
        }
        while ((channel.read())[0] != (byte) 0xFE /* eof */) { /* skip */ }
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE /* eof */; ) {
            resultSet.add(new ResultSetRowPacket(bytes));
        }
        return resultSet.toArray(new ResultSetRowPacket[0]);
    }

    private void closeChannel(final PacketChannel channel) throws IOException {
        connected = false;
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    private void notifyEventListeners(Event event) {
        if (event.getData() instanceof EventDeserializer.EventDataWrapper) {
            event = new Event(event.getHeader(), ((EventDeserializer.EventDataWrapper) event.getData()).getExternal());
        }
        for (BinaryLogRemoteClient.EventListener eventListener : eventListeners) {
            try {
                eventListener.onEvent(event);
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn(eventListener + " choked on " + event, e);
                }
            }
        }
    }

    private void notifyConnectEvent() {
        for (BinaryLogRemoteClient.LifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onConnect(this);
        }
    }

    private void notifyDisconnectEvent() {
        for (BinaryLogRemoteClient.LifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onDisconnect(this);
        }
    }

    @Override
    public String getBinlogFilename() {
        return binlogFilename;
    }

    @Override
    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    @Override
    public long getBinlogPosition() {
        return binlogPosition;
    }

    @Override
    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public SSLMode getSSLMode() {
        return sslMode;
    }

    public void setSSLMode(SSLMode sslMode) {
        if (sslMode == null) {
            throw new IllegalArgumentException("SSL mode cannot be NULL");
        }
        this.sslMode = sslMode;
    }

    /**
     * @return GTID set. Note that this value changes with each received GTID event (provided client is in GTID mode).
     * @see #setGtidSet(String)
     */
    public String getGtidSet() {
        synchronized (gtidSetAccessLock) {
            return gtidSet != null ? gtidSet.toString() : null;
        }
    }

    /**
     * @param gtidSet GTID set (can be an empty string).
     *                <p>NOTE #1: Any value but null will switch BinaryLogRemoteClient into a GTID mode (this will also set binlogFilename
     *                to "" (provided it's null) forcing MySQL to send events starting from the oldest known binlog (keep in mind that
     *                connection will fail if gtid_purged is anything but empty (unless {@link #setGtidSetFallbackToPurged(boolean)} is set
     *                to true))).
     *                <p>NOTE #2: GTID set is automatically updated with each incoming GTID event (provided GTID mode is on).
     * @see #getGtidSet()
     * @see #setGtidSetFallbackToPurged(boolean)
     */
    public void setGtidSet(String gtidSet) {
        if (gtidSet != null && this.binlogFilename == null) {
            this.binlogFilename = "";
        }
        synchronized (gtidSetAccessLock) {
            this.gtidSet = gtidSet != null ? new GtidSet(gtidSet) : null;
        }
    }

    /**
     * @see #setGtidSetFallbackToPurged(boolean)
     */
    public boolean isGtidSetFallbackToPurged() {
        return gtidSetFallbackToPurged;
    }

    /**
     * @param gtidSetFallbackToPurged true if gtid_purged should be used as a fallback when gtidSet is set to "" and MySQL server has purged
     *                                some of the binary logs, false otherwise (default).
     */
    public void setGtidSetFallbackToPurged(boolean gtidSetFallbackToPurged) {
        this.gtidSetFallbackToPurged = gtidSetFallbackToPurged;
    }

    /**
     * @see #setUseBinlogFilenamePositionInGtidMode(boolean)
     */
    public boolean isUseBinlogFilenamePositionInGtidMode() {
        return useBinlogFilenamePositionInGtidMode;
    }

    /**
     * @param useBinlogFilenamePositionInGtidMode true if MySQL server should start streaming events from a given {@link
     *                                            #getBinlogFilename()} and {@link #getBinlogPosition()} instead of "the oldest known
     *                                            binlog" when {@link #getGtidSet()} is set, false otherwise (default).
     */
    public void setUseBinlogFilenamePositionInGtidMode(boolean useBinlogFilenamePositionInGtidMode) {
        this.useBinlogFilenamePositionInGtidMode = useBinlogFilenamePositionInGtidMode;
    }

    /**
     * @param eventDeserializer custom event deserializer
     */
    public void setEventDeserializer(EventDeserializer eventDeserializer) {
        if (eventDeserializer == null) {
            throw new IllegalArgumentException("Event deserializer cannot be NULL");
        }
        this.eventDeserializer = eventDeserializer;
    }

    public interface EventListener {

        void onEvent(Event event);
    }

    public interface LifecycleListener {

        /**
         * Called once client has successfully logged in but before started to receive binlog events.
         */
        void onConnect(BinaryLogRemoteClient client);

        /**
         * It's guarantied to be called before {@link #onDisconnect(BinaryLogRemoteClient)}) in case of communication failure.
         */
        void onCommunicationFailure(BinaryLogRemoteClient client, Exception ex);

        /**
         * Called in case of failed event deserialization. Note this type of error does NOT cause client to disconnect. If you wish to stop
         * receiving events you'll need to fire client.disconnect() manually.
         */
        void onEventDeserializationFailure(BinaryLogRemoteClient client, Exception ex);

        /**
         * Called upon disconnect (regardless of the reason).
         */
        void onDisconnect(BinaryLogRemoteClient client);
    }

}