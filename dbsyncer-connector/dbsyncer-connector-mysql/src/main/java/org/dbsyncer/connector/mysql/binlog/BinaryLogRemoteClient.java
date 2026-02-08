/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.binlog;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.deserializer.DeleteDeserializer;
import org.dbsyncer.connector.mysql.deserializer.ExtDeleteDeserializer;
import org.dbsyncer.connector.mysql.deserializer.ExtUpdateDeserializer;
import org.dbsyncer.connector.mysql.deserializer.ExtWriteDeserializer;
import org.dbsyncer.connector.mysql.deserializer.UpdateDeserializer;
import org.dbsyncer.connector.mysql.deserializer.WriteDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BinaryLogRemoteClient implements BinaryLogClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final SSLSocketFactory DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory() {

        @Override
        protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
            sc.init(null, new TrustManager[]{new X509TrustManager() {

                @Override
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }}, null);
        }
    };
    private static final SSLSocketFactory DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory();

    // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
    private static final int MAX_PACKET_LENGTH = 16777215;

    private final String hostname;
    private final int port;
    private final String schema;
    private final String username;
    private final String password;
    private SSLMode sslMode = SSLMode.DISABLED;

    private EventDeserializer eventDeserializer;
    private Map<Long, TableMapEventData> tableMapEventByTableId;
    private final boolean blocking = true;
    private long serverId = 65535L;
    private volatile String binlogFilename;
    private volatile long binlogPosition = 4;
    private volatile long connectionId;
    private volatile PacketChannel channel;
    private volatile boolean connected;
    private volatile boolean connectedError;
    private Thread worker;
    private Thread keepAlive;
    private String workerThreadName;

    private final Lock connectLock = new ReentrantLock();
    private boolean gtidEnabled = false;
    private final Object gtidSetAccessLock = new Object();
    private GtidSet gtidSet;
    private String gtid;
    private boolean tx;
    private boolean gtidSetFallbackToPurged;
    private boolean useBinlogFilenamePositionInGtidMode;
    private Boolean isMariaDB;

    private static final long MYSQL_VERSION_8_4 = 8400000000L;
    private final List<BinaryLogRemoteClient.EventListener> eventListeners = new CopyOnWriteArrayList<>();
    private final List<BinaryLogRemoteClient.LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<>();

    /**
     * Alias for BinaryLogRemoteClient(hostname, port, &lt;no schema&gt; = null, username, password).
     *
     * @see BinaryLogRemoteClient#BinaryLogRemoteClient(String, int, String, String, String, long)
     */
    public BinaryLogRemoteClient(String hostname, int port, String username, String password) throws IOException {
        this(hostname, port, null, username, password, 0L);
    }

    /**
     * @param hostname mysql server hostname
     * @param port     mysql server port
     * @param schema   database name, nullable. Note that this parameter has nothing to do with event filtering. It's used only during the
     *                 authentication.
     * @param username login name
     * @param password password
     * @param serverId serverId
     */
    public BinaryLogRemoteClient(String hostname, int port, String schema, String username, String password, long serverId) throws IOException {
        this.hostname = hostname;
        this.port = port;
        this.schema = schema;
        this.username = username;
        this.password = password;
        this.serverId = randomPort(serverId);
    }

    @Override
    public void connect() throws Exception {
        try {
            connectLock.lock();
            if (connected) {
                throw new IllegalStateException("BinaryLogRemoteClient is already connected");
            }
            setConfig();
            openChannel();
            connected = true;
            // new keepalive thread
            spawnKeepAliveThread();

            // dump binary log
            requestBinaryLogStream();
            ensureEventDeserializerHasRequiredEDDs();

            // new listen thread
            spawnWorkerThread();
            lifecycleListeners.forEach(listener->listener.onConnect(this));
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void disconnect() throws Exception {
        if (connected) {
            try {
                connectLock.lock();
                closeChannel(channel);
                connected = false;
                if (null != this.worker && !worker.isInterrupted()) {
                    this.worker.interrupt();
                    this.worker = null;
                }
                if (null != this.keepAlive && !keepAlive.isInterrupted()) {
                    this.keepAlive.interrupt();
                    this.keepAlive = null;
                }
                lifecycleListeners.forEach(listener->listener.onDisconnect(this));
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

    private String createClientId() {
        return hostname + ":" + port + "_" + connectionId;
    }

    private void openChannel() throws IOException {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(hostname, port), 3000);
            channel = new PacketChannel(socket);
            if (channel.getInputStream().peek() == -1) {
                throw new EOFException();
            }
        } catch (IOException e) {
            closeChannel(channel);
            throw new IOException("Failed to connect to MySQL on " + hostname + ":" + port + ". Please make sure it's running.", e);
        }
        GreetingPacket greetingPacket = receiveGreeting(channel);
        detectMariaDB(greetingPacket);
        tryUpgradeToSSL(greetingPacket);
        new Authenticator(greetingPacket, channel, schema, username, password).authenticate();
        channel.authenticationComplete();

        connectionId = greetingPacket.getThreadId();
        if ("".equals(binlogFilename)) {
            setupGtidSet();
        }
        if (binlogFilename == null) {
            fetchBinlogFilenameAndPosition();
        } else {
            // 校验请求的binlog文件是否仍然存在,若不存在则回退到最新
            validateRequestedBinlogOrFallback();
        }
        if (binlogPosition < 4) {
            logger.warn("Binary log position adjusted from {} to {}", binlogPosition, 4);
            binlogPosition = 4;
        }
        ChecksumType checksumType = fetchBinlogChecksum();
        if (checksumType != ChecksumType.NONE) {
            confirmSupportOfChecksum(channel, checksumType);
        }
        synchronized (gtidSetAccessLock) {
            String position = gtidSet != null ? gtidSet.toString() : binlogFilename + "/" + binlogPosition;
            logger.info("Connected to {}:{} at {} (sid:{}, cid:{})", hostname, port, position, serverId, connectionId);
        }
    }

    private void validateRequestedBinlogOrFallback() throws IOException {
        channel.write(new QueryCommand("SHOW BINARY LOGS"));
        ResultSetRowPacket[] logs = readResultSet();
        if (logs.length == 0) {
            return;
        }
        boolean exists = Arrays.stream(logs).anyMatch(r->binlogFilename.equals(r.getValue(0)));
        if (!exists) {
            logger.warn("Requested binlog {} no longer exists, fallback to latest.", binlogFilename);
            fetchBinlogFilenameAndPosition();
        }
    }

    private void detectMariaDB(GreetingPacket packet) {
        String serverVersion = packet.getServerVersion();
        if (serverVersion == null) {
            return;
        }
        this.isMariaDB = serverVersion.toLowerCase().contains("mariadb");
    }

    private boolean tryUpgradeToSSL(GreetingPacket greetingPacket) throws IOException {
        if (sslMode != SSLMode.DISABLED) {
            boolean serverSupportsSSL = (greetingPacket.getServerCapabilities() & ClientCapabilities.SSL) != 0;
            if (!serverSupportsSSL && (sslMode == SSLMode.REQUIRED || sslMode == SSLMode.VERIFY_CA || sslMode == SSLMode.VERIFY_IDENTITY)) {
                throw new IOException("MySQL server does not support SSL");
            }
            if (serverSupportsSSL) {
                SSLRequestCommand sslRequestCommand = new SSLRequestCommand();
                int collation = greetingPacket.getServerCollation();
                sslRequestCommand.setCollation(collation);
                channel.write(sslRequestCommand);
                SSLSocketFactory sslSocketFactory = sslMode == SSLMode.REQUIRED || sslMode == SSLMode.PREFERRED ? DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY : DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY;
                channel.upgradeToSSL(sslSocketFactory, sslMode == SSLMode.VERIFY_IDENTITY ? new TLSHostnameVerifier() : null);
                logger.info("SSL enabled");
                return true;
            }
        }
        return false;
    }

    private void listenForEventPackets(final PacketChannel channel) {
        ByteArrayInputStream inputStream = channel.getInputStream();
        try {
            while (inputStream.peek() != -1) {
                int packetLength = inputStream.readInteger(3);
                // 1 byte for sequence
                inputStream.skip(1);
                int marker = inputStream.read();
                // 255 error
                if (marker == 0xFF) {
                    ErrorPacket errorPacket = new ErrorPacket(inputStream.read(packetLength - 1));
                    throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
                }
                // 254 empty
                if (marker == 0xFE && !blocking) {
                    break;
                }
                Event event = eventDeserializer.nextEvent(packetLength == MAX_PACKET_LENGTH ? new ByteArrayInputStream(readPacketSplitInChunks(inputStream, packetLength - 1)) : inputStream);
                if (event != null) {
                    updateGtidSet(event);
                    notifyEventListeners(event);
                    updateClientBinlogFilenameAndPosition(event);
                    continue;
                }
                throw new EOFException("event data deserialization exception");
            }
        } catch (Exception e) {
            notifyException(e);
        }
    }

    private void ensureEventDeserializerHasRequiredEDDs() {
        ensureEventDataDeserializerIfPresent(EventType.ROTATE, RotateEventDataDeserializer.class);
        synchronized (gtidSetAccessLock) {
            if (this.gtidEnabled) {
                ensureEventDataDeserializerIfPresent(EventType.GTID, GtidEventDataDeserializer.class);
                ensureEventDataDeserializerIfPresent(EventType.QUERY, QueryEventDataDeserializer.class);
                ensureEventDataDeserializerIfPresent(EventType.ANNOTATE_ROWS, AnnotateRowsEventDataDeserializer.class);
                ensureEventDataDeserializerIfPresent(EventType.MARIADB_GTID, MariadbGtidEventDataDeserializer.class);
                ensureEventDataDeserializerIfPresent(EventType.MARIADB_GTID_LIST, MariadbGtidListEventDataDeserializer.class);
            }
        }
    }

    protected void checkError(byte[] packet) throws IOException {
        if (packet[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(packet, 1, packet.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
        }
    }

    private GreetingPacket receiveGreeting(final PacketChannel channel) throws IOException {
        byte[] initialHandshakePacket = channel.read();
        /* error */
        if (initialHandshakePacket[0] == (byte) 0xFF) {
            byte[] bytes = Arrays.copyOfRange(initialHandshakePacket, 1, initialHandshakePacket.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
        }
        return new GreetingPacket(initialHandshakePacket);
    }

    private void requestBinaryLogStream() throws IOException {
        long serverId = blocking ? this.serverId : 0; // http://bugs.mysql.com/bug.php?id=71178
        if (this.isMariaDB) {
            requestBinaryLogStreamMaria(serverId);
            return;
        }

        requestBinaryLogStreamMysql(serverId);
    }

    private void requestBinaryLogStreamMysql(long serverId) throws IOException {
        Command dumpBinaryLogCommand;
        synchronized (gtidSetAccessLock) {
            if (this.gtidEnabled) {
                dumpBinaryLogCommand = new DumpBinaryLogGtidCommand(serverId, useBinlogFilenamePositionInGtidMode ? binlogFilename : "", useBinlogFilenamePositionInGtidMode ? binlogPosition : 4,
                        gtidSet);
            } else {
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, binlogFilename, binlogPosition);
            }
        }
        channel.write(dumpBinaryLogCommand);
    }

    private void requestBinaryLogStreamMaria(long serverId) throws IOException {
        Command dumpBinaryLogCommand;
        synchronized (gtidSetAccessLock) {
            if (this.gtidEnabled) {
                channel.write(new QueryCommand("SET @mariadb_slave_capability=4"));
                checkError(channel.read());
                logger.info(gtidSet.toString());
                channel.write(new QueryCommand("SET @slave_connect_state = '" + gtidSet.toString() + "'"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_strict_mode = 0"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_ignore_duplicates = 0"));
                checkError(channel.read());
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, "", 0L, false);
            } else {
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, binlogFilename, binlogPosition);
            }
        }
        channel.write(dumpBinaryLogCommand);
    }

    private void ensureEventDataDeserializerIfPresent(EventType eventType, Class<? extends EventDataDeserializer<?>> eventDataDeserializerClass) {
        EventDataDeserializer<?> eventDataDeserializer = eventDeserializer.getEventDataDeserializer(eventType);
        if (eventDataDeserializer.getClass() != eventDataDeserializerClass && eventDataDeserializer.getClass() != EventDeserializer.EventDataWrapper.Deserializer.class) {
            EventDataDeserializer<?> internalEventDataDeserializer;
            try {
                internalEventDataDeserializer = eventDataDeserializerClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            eventDeserializer.setEventDataDeserializer(eventType, new EventDeserializer.EventDataWrapper.Deserializer(internalEventDataDeserializer, eventDataDeserializer));
        }
    }

    private String fetchGtidPurged() throws IOException {
        channel.write(new QueryCommand("show global variables like 'gtid_purged'"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length != 0) {
            return resultSet[0].getValue(1).toUpperCase();
        }
        return "";
    }

    private void setupGtidSet() throws IOException {
        if (!this.gtidEnabled)
            return;

        synchronized (gtidSetAccessLock) {
            if (this.isMariaDB) {
                if (gtidSet == null) {
                    gtidSet = new MariadbGtidSet("");
                } else if (!(gtidSet instanceof MariadbGtidSet)) {
                    throw new RuntimeException("Connected to MariaDB but given a mysql GTID set!");
                }
            } else {
                if (gtidSet == null && gtidSetFallbackToPurged) {
                    gtidSet = new GtidSet(fetchGtidPurged());
                } else if (gtidSet == null) {
                    gtidSet = new GtidSet("");
                } else if (gtidSet instanceof MariadbGtidSet) {
                    throw new RuntimeException("Connected to Mysql but given a MariaDB GTID set!");
                }
            }
        }

    }

    private Long getVersion() throws IOException {
        channel.write(new QueryCommand("SELECT VERSION()"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length == 0) {
            throw new IOException("Failed to getVersion, command SELECT VERSION()");
        }
        ResultSetRowPacket resultSetRow = resultSet[0];
        String version = resultSetRow.getValue(0).replace(StringUtil.POINT, StringUtil.EMPTY);
        if (version.contains(StringUtil.HORIZONTAL)) {
            version = version.split(StringUtil.HORIZONTAL)[0];
        }
        return Long.parseLong(String.format("%-10s", version).replace(StringUtil.SPACE, "0"));
    }

    private void fetchBinlogFilenameAndPosition() throws IOException {
        if (getVersion() >= MYSQL_VERSION_8_4) {
            channel.write(new QueryCommand("SHOW BINARY LOG STATUS"));
        } else {
            channel.write(new QueryCommand("show master status"));
        }

        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length == 0) {
            throw new IOException("Failed to determine binlog filename/position");
        }
        ResultSetRowPacket resultSetRow = resultSet[0];
        binlogFilename = resultSetRow.getValue(0);
        binlogPosition = Long.parseLong(resultSetRow.getValue(1));
    }

    private ChecksumType fetchBinlogChecksum() throws IOException {
        channel.write(new QueryCommand("show global variables like 'binlog_checksum'"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length == 0) {
            return ChecksumType.NONE;
        }
        return ChecksumType.valueOf(resultSet[0].getValue(1).toUpperCase());
    }

    private void confirmSupportOfChecksum(final PacketChannel channel, ChecksumType checksumType) throws IOException {
        channel.write(new QueryCommand("set @master_binlog_checksum= @@global.binlog_checksum"));
        byte[] statementResult = channel.read();
        checkError(statementResult);
        eventDeserializer.setChecksumType(checksumType);
    }

    private byte[] readPacketSplitInChunks(ByteArrayInputStream inputStream, int packetLength) throws IOException {
        byte[] result = inputStream.read(packetLength);
        int chunkLength;
        do {
            chunkLength = inputStream.readInteger(3);
            // noinspection ResultOfMethodCallIgnored
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

    private ResultSetRowPacket[] readResultSet() throws IOException {
        List<ResultSetRowPacket> resultSet = new LinkedList();
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
        }
        while ((channel.read())[0] != (byte) 0xFE /* eof */) {
            /* skip */ }
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE /* eof */;) {
            resultSet.add(new ResultSetRowPacket(bytes));
        }
        return resultSet.toArray(new ResultSetRowPacket[0]);
    }

    private void closeChannel(final PacketChannel channel) throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    private void setConfig() {
        if (null == tableMapEventByTableId) {
            tableMapEventByTableId = new HashMap<>();
        }

        IdentityHashMap eventDataDeserializers = new IdentityHashMap();
        if (null == eventDeserializer) {
            this.eventDeserializer = new EventDeserializer(new EventHeaderV4Deserializer(), new NullEventDataDeserializer(), eventDataDeserializers, tableMapEventByTableId);
        }

        // Process event priority: RotateEvent > FormatDescriptionEvent > TableMapEvent > RowsEvent > XidEvent
        eventDataDeserializers.put(EventType.ROTATE, new RotateEventDataDeserializer());
        eventDataDeserializers.put(EventType.FORMAT_DESCRIPTION, new FormatDescriptionEventDataDeserializer());
        eventDataDeserializers.put(EventType.TABLE_MAP, new TableMapEventDataDeserializer());
        eventDataDeserializers.put(EventType.UPDATE_ROWS, new UpdateDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.WRITE_ROWS, new WriteDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.DELETE_ROWS, new DeleteDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.EXT_WRITE_ROWS, (new ExtWriteDeserializer(tableMapEventByTableId)).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.EXT_UPDATE_ROWS, (new ExtUpdateDeserializer(tableMapEventByTableId)).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.EXT_DELETE_ROWS, (new ExtDeleteDeserializer(tableMapEventByTableId)).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.XID, new XidEventDataDeserializer());
        eventDataDeserializers.put(EventType.INTVAR, new IntVarEventDataDeserializer());
        eventDataDeserializers.put(EventType.QUERY, new QueryEventDataDeserializer());
        eventDataDeserializers.put(EventType.ROWS_QUERY, new RowsQueryEventDataDeserializer());
        eventDataDeserializers.put(EventType.GTID, new GtidEventDataDeserializer());
        eventDataDeserializers.put(EventType.PREVIOUS_GTIDS, new PreviousGtidSetDeserializer());
        eventDataDeserializers.put(EventType.XA_PREPARE, new XAPrepareEventDataDeserializer());
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

    private long randomPort(long serverId) throws IOException {
        if (0 == serverId) {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(0);
                return serverSocket.getLocalPort();
            } finally {
                if (null != serverSocket) {
                    serverSocket.close();
                }
            }
        }
        return serverId;
    }

    private void spawnWorkerThread() {
        this.worker = new Thread(()->listenForEventPackets(channel));
        this.worker.setDaemon(false);
        this.workerThreadName = "binlog-parser-" + createClientId();
        this.worker.setName(workerThreadName);
        this.worker.start();
    }

    private void spawnKeepAliveThread() {
        String clientId = createClientId();
        this.keepAlive = new Thread(()-> {
            while (connected) {
                if (connectedError) {
                    break;
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                    channel.write(new PingCommand());
                } catch (Exception e) {
                    notifyException(e);
                    break;
                }
            }
            while (connectedError) {
                try {
                    logger.info("Trying to restore lost connection to {}}", createClientId());
                    if (!connected) {
                        logger.warn("Trying to stop");
                        break;
                    }
                    disconnect();
                    connect();
                    connectedError = false;
                    break;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
        this.keepAlive.setDaemon(false);
        this.keepAlive.setName("binlog-keepalive-" + clientId);
        this.keepAlive.start();
    }

    private void notifyException(Exception e) {
        if (connected) {
            logger.error(e.getMessage(), e);
            if (e instanceof ServerException) {
                ServerException serverException = (ServerException) e;
                // 若binlog文件不存在（错误码1236），下次重连时回退到最新位置
                if (serverException.getErrorCode() == 1236) {
                    binlogFilename = null;
                    binlogPosition = 0;
                }
            }
            connectedError = true;
            lifecycleListeners.forEach(listener->listener.onException(this, e));
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

    @Override
    public EventDeserializer getEventDeserializer() {
        return eventDeserializer;
    }

    @Override
    public void setEventDeserializer(EventDeserializer eventDeserializer) {
        if (eventDeserializer == null) {
            throw new IllegalArgumentException("Event deserializer cannot be NULL");
        }
        this.eventDeserializer = eventDeserializer;
    }

    @Override
    public Map<Long, TableMapEventData> getTableMapEventByTableId() {
        return tableMapEventByTableId;
    }

    @Override
    public void setTableMapEventByTableId(Map<Long, TableMapEventData> tableMapEventByTableId) {
        this.tableMapEventByTableId = tableMapEventByTableId;
    }

    @Override
    public String getWorkerThreadName() {
        return workerThreadName;
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
     * @param gtidStr GTID set string (can be an empty string).
     * <p>NOTE #1: Any value but null will switch BinaryLogRemoteClient into a GTID mode (this will also set binlogFilename
     * to "" (provided it's null) forcing MySQL to send events starting from the oldest known binlog (keep in mind
     * that connection will fail if gtid_purged is anything but empty (unless
     * {@link #setGtidSetFallbackToPurged(boolean)} is set to true))).
     * <p>NOTE #2: GTID set is automatically updated with each incoming GTID event (provided GTID mode is on).
     * @see #getGtidSet()
     * @see #setGtidSetFallbackToPurged(boolean)
     */
    public void setGtidSet(String gtidStr) {
        if (gtidStr == null)
            return;

        this.gtidEnabled = true;

        if (this.binlogFilename == null) {
            this.binlogFilename = "";
        }
        synchronized (gtidSetAccessLock) {
            if (!gtidStr.equals("")) {
                if (MariadbGtidSet.isMariaGtidSet(gtidStr)) {
                    this.gtidSet = new MariadbGtidSet(gtidStr);
                } else {
                    this.gtidSet = new GtidSet(gtidStr);
                }
            }
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
        void onException(BinaryLogRemoteClient client, Exception ex);

        /**
         * Called upon disconnect (regardless of the reason).
         */
        void onDisconnect(BinaryLogRemoteClient client);
    }

}