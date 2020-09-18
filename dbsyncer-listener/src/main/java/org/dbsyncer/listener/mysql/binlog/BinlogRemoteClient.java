package org.dbsyncer.listener.mysql.binlog;

import org.dbsyncer.listener.mysql.binlog.impl.ReplicationBasedBinlogParser;
import org.dbsyncer.listener.mysql.binlog.impl.parser.*;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.io.impl.SocketFactoryImpl;
import org.dbsyncer.listener.mysql.net.Packet;
import org.dbsyncer.listener.mysql.net.Transport;
import org.dbsyncer.listener.mysql.net.TransportException;
import org.dbsyncer.listener.mysql.net.impl.AuthenticatorImpl;
import org.dbsyncer.listener.mysql.net.impl.Query;
import org.dbsyncer.listener.mysql.net.impl.TransportImpl;
import org.dbsyncer.listener.mysql.net.impl.packet.ErrorPacket;
import org.dbsyncer.listener.mysql.net.impl.packet.command.ComBinlogDumpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <h3>远程解析器</h3>
 * <ol type="1">
 * <li>
 * <dt>binlog文件解析器</dt></li>
 * <dd>远程连接数据库,监听binlog增量日志</dd>
 * </ol>
 *
 * @ClassName: BinlogRemoteClient
 * @author: AE86
 * @date: 2018年10月17日 下午2:51:43
 */
public class BinlogRemoteClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogRemoteClient.class);

    protected int serverId = 65535;
    protected int port = 3306;
    protected String host = "127.0.0.1";
    protected String user = "root";
    protected String password = "root";
    protected String threadSuffixName;
    protected String encoding = "utf-8";
    protected String binlogFileName;
    protected long binlogPosition = 4;

    protected Float heartbeatPeriod;
    protected Transport transport;
    protected BinlogRowEventFilter filter;
    protected ReplicationBasedBinlogParser binlogParser;
    protected List<BinlogParserListener> binlogParserListener = new CopyOnWriteArrayList<BinlogParserListener>();
    protected BinlogEventListener binlogEventListener;

    protected String uuid = UUID.randomUUID().toString();
    // 当发生异常的是否停止线程,默认false直接终止
    protected boolean stopOnEOF = false;
    protected int level1BufferSize = 1024 * 1024;
    protected int level2BufferSize = 8 * 1024 * 1024;
    protected int socketReceiveBufferSize = 512 * 1024;
    protected final AtomicBoolean running = new AtomicBoolean(false);

    public BinlogRemoteClient() {
        super();
    }

    public BinlogRemoteClient(String host, int port, String user, String password, String threadSuffixName) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.threadSuffixName = threadSuffixName;
    }

    public boolean isRunning() {
        return this.running.get();
    }

    public void start() throws Exception {
        // 1、检查启动状态
        if (!this.running.compareAndSet(false, true)) {
            return;
        }

        // 2、创建通信管道
        if (this.transport == null) {
            this.transport = getDefaultTransport();
        }
        // 尝试连接
        this.transport.connect(this.host, this.port);

        // 3、配置解析器
        if (this.binlogParser == null) {
            this.binlogParser = getSimpleBinlogParser();
        }
        this.binlogParser.setTransport(this.transport);
        this.binlogParser.setBinlogFileName(this.binlogFileName);
        this.binlogParser.setEventListener(this.binlogEventListener);
        this.binlogParserListener.add(new BinlogParserListener.Adapter() {
            @Override
            public void onStop(BinlogParser parser) {
                try {
                    stopQuietly(0, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    LOGGER.error(e.getLocalizedMessage());
                }
            }
        });
        this.binlogParser.setParserListeners(this.binlogParserListener);
        binlogParserListener = null;

        // 4、检查连接数据库的基本信息
        final Query query = new Query(this.transport);
        setupFilenameAndPosition(query);
        setupChecksumState(query);
        setupHeartbeatPeriod(query);
        setupSlaveUUID(query);
        dumpBinlog();

        // 5、启动解析器
        this.binlogParser.start(threadSuffixName);
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        if (!this.running.compareAndSet(true, false)) {
            return;
        }

        // disconnect the transport first: seems kinda wrong, but the parser
        // thread can be blocked waiting for the
        // last event, and doesn't have any timeouts. so we deal with the EOF
        // exception thrown elsewhere in the code.
        this.transport.disconnect();

        if(null != this.binlogParser){
            this.binlogParser.stop(timeout, unit);
        }
    }

    public void stopQuietly() throws Exception {
        stop(0, TimeUnit.MILLISECONDS);
    }

    public void stopQuietly(long timeout, TimeUnit unit) throws Exception {
        stop(timeout, unit);
    }

    private Transport getDefaultTransport() throws Exception {
        final TransportImpl r = new TransportImpl();
        r.setLevel1BufferSize(this.level1BufferSize);
        r.setLevel2BufferSize(this.level2BufferSize);

        final AuthenticatorImpl authenticator = new AuthenticatorImpl();
        authenticator.setUser(this.user);
        authenticator.setPassword(this.password);
        authenticator.setEncoding(this.encoding);
        r.setAuthenticator(authenticator);

        final SocketFactoryImpl socketFactory = new SocketFactoryImpl();
        socketFactory.setKeepAlive(true);
        socketFactory.setTcpNoDelay(false);
        socketFactory.setReceiveBufferSize(this.socketReceiveBufferSize);
        r.setSocketFactory(socketFactory);
        return r;
    }

    private void setupFilenameAndPosition(Query query) throws Exception {
        if (null == binlogFileName) {
            try {
                List<String> cols = query.getFirst("show master status");
                binlogFileName = cols.get(0);
                binlogPosition = Long.parseLong(cols.get(1));
            } catch (TransportException e) {
                // ignore no-such-variable errors on mysql 5.5
                if (e.getErrorCode() != 1193)
                    throw e;
            }
        }
        if (binlogPosition < 4) {
            binlogPosition = 4;
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Binary log position adjusted from " + binlogPosition + " to " + binlogPosition);
            }
        }
    }

    private void setupChecksumState(Query query) throws Exception {
        try {
            List<String> cols = query.getFirst("SELECT @@global.binlog_checksum");

            if (cols != null && cols.get(0).equals("CRC32") || cols.get(0).equals("NONE")) {
                query.getFirst("SET @master_binlog_checksum = @@global.binlog_checksum");
            }
        } catch (TransportException e) {
            // ignore no-such-variable errors on mysql 5.5
            if (e.getErrorCode() != 1193)
                throw e;
        }
    }

    private void setupHeartbeatPeriod(Query query) throws Exception {
        if (this.heartbeatPeriod == null)
            return;

        BigInteger nanoSeconds = BigDecimal.valueOf(1000000000).multiply(BigDecimal.valueOf(this.heartbeatPeriod))
                .toBigInteger();
        query.getFirst("SET @master_heartbeat_period = " + nanoSeconds);
    }

    private void setupSlaveUUID(Query query) throws Exception {
        query.getFirst("SET @slave_uuid = '" + this.uuid + "'");
    }

    private void dumpBinlog() throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("starting replication at %s:%d", this.binlogFileName, this.binlogPosition));
        }
        final ComBinlogDumpPacket command = new ComBinlogDumpPacket();
        command.setBinlogFlag(0);
        command.setServerId(this.serverId);
        command.setBinlogPosition(this.binlogPosition);
        command.setBinlogFileName(StringColumn.valueOf(this.binlogFileName.getBytes(this.encoding)));
        this.transport.getOutputStream().writePacket(command);
        this.transport.getOutputStream().flush();

        final Packet packet = this.transport.getInputStream().readPacket();
        if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
            final ErrorPacket error = ErrorPacket.valueOf(packet);
            throw new TransportException(error);
        }
    }

    private ReplicationBasedBinlogParser getSimpleBinlogParser() throws Exception {
        final ReplicationBasedBinlogParser r = new ReplicationBasedBinlogParser(stopOnEOF, threadSuffixName);
        r.registerEventParser(new RotateEventParser());
        r.registerEventParser(new FormatDescriptionEventParser());
        r.registerEventParser(new XidEventParser());
        r.registerEventParser(new TableMapEventParser());
        r.registerEventParser(new WriteRowsEventV2Parser().setRowEventFilter(filter));
        r.registerEventParser(new UpdateRowsEventV2Parser().setRowEventFilter(filter));
        r.registerEventParser(new DeleteRowsEventV2Parser().setRowEventFilter(filter));
        return r;
    }

    private ReplicationBasedBinlogParser getDefaultBinlogParser() throws Exception {
        ReplicationBasedBinlogParser r = getSimpleBinlogParser();
        r.registerEventParser(new StopEventParser());
        r.registerEventParser(new IntvarEventParser());
        r.registerEventParser(new RandEventParser());
        r.registerEventParser(new QueryEventParser());
        r.registerEventParser(new UserVarEventParser());
        r.registerEventParser(new IncidentEventParser());
        r.registerEventParser(new WriteRowsEventParser());
        r.registerEventParser(new UpdateRowsEventParser());
        r.registerEventParser(new DeleteRowsEventParser());
        r.registerEventParser(new GtidEventParser());
        return r;
    }

    /**
     * 设置简化版解析器
     */
    public void setupDefaultBinlogParser() {
        try {
            this.binlogParser = getDefaultBinlogParser();
        } catch (Exception e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }

    public void setHeartbeatPeriod(float period) {
        this.heartbeatPeriod = period;
    }

    public Float getHeartbeatPeriod() {
        return this.heartbeatPeriod;
    }

    public long getHeartbeatCount() {
        return binlogParser.getHeartbeatCount();
    }

    public Long millisSinceLastEvent() {
        return binlogParser.millisSinceLastEvent();
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }


    public String getBinlogFileName() {
        return binlogFileName;
    }


    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }


    public int getLevel1BufferSize() {
        return level1BufferSize;
    }


    public void setLevel1BufferSize(int level1BufferSize) {
        this.level1BufferSize = level1BufferSize;
    }


    public int getLevel2BufferSize() {
        return level2BufferSize;
    }


    public void setLevel2BufferSize(int level2BufferSize) {
        this.level2BufferSize = level2BufferSize;
    }


    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }


    public void setSocketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
    }


    public Transport getTransport() {
        return transport;
    }


    public void setTransport(Transport transport) {
        this.transport = transport;
    }


    public void setFilter(BinlogRowEventFilter filter) {
        this.filter = filter;
    }

    public BinlogParser getBinlogParser() {
        return binlogParser;
    }

    public void setBinlogParser(ReplicationBasedBinlogParser parser) {
        this.binlogParser = parser;
    }

    public void setBinlogParserListener(BinlogParserListener listener) {
        binlogParserListener.add(listener);
    }

    public BinlogEventListener getBinlogEventListener() {
        return binlogEventListener;
    }

    public void setBinlogEventListener(BinlogEventListener listener) {
        this.binlogEventListener = listener;
    }


    public void setThreadSuffixName(String threadSuffixName) {
        this.threadSuffixName = threadSuffixName;
    }


    public void setStopOnEOF(boolean stopOnEOF) {
        this.stopOnEOF = stopOnEOF;
    }
}
