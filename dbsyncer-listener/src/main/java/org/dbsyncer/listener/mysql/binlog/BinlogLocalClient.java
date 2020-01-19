package org.dbsyncer.listener.mysql.binlog;

import org.dbsyncer.listener.mysql.binlog.impl.FileBasedBinlogParser;
import org.dbsyncer.listener.mysql.binlog.impl.parser.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <h3>本地解析器</h3>
 * <ol type="1">
 * <li><dt>binlog文件解析器</dt></li>
 * <dd>监听本地binlog文件增量数据</dd>
 * </ol>
 *
 * @ClassName: BinlogLocalClient
 * @author: AE86
 * @date: 2018年10月17日 下午2:50:38
 */
public class BinlogLocalClient {
    protected long stopPosition;
    protected long startPosition;
    protected String binlogFileName;
    protected String binlogFilePath;

    protected BinlogParser binlogParser;
    protected BinlogEventListener binlogEventListener;
    protected final AtomicBoolean running = new AtomicBoolean(false);
    protected String threadSuffixName = "binlog-parser";

    public boolean isRunning() {
        return this.running.get();
    }

    public void start() throws Exception {
        if (!this.running.compareAndSet(false, true)) {
            return;
        }

        if (this.binlogParser == null) this.binlogParser = getDefaultBinlogParser();
        this.binlogParser.setEventListener(this.binlogEventListener);
        this.binlogParser.start(threadSuffixName);
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        if (!this.running.compareAndSet(true, false)) {
            return;
        }

        this.binlogParser.stop(timeout, unit);
    }

    public long getStopPosition() {
        return stopPosition;
    }

    public void setStopPosition(long position) {
        this.stopPosition = position;
    }

    public long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(long position) {
        this.startPosition = position;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public void setBinlogFileName(String name) {
        this.binlogFileName = name;
    }

    public String getBinlogFilePath() {
        return binlogFilePath;
    }

    public void setBinlogFilePath(String path) {
        this.binlogFilePath = path;
    }

    public BinlogParser getBinlogParser() {
        return binlogParser;
    }

    public void setBinlogParser(BinlogParser parser) {
        this.binlogParser = parser;
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

    protected FileBasedBinlogParser getDefaultBinlogParser() throws Exception {
        final FileBasedBinlogParser r = new FileBasedBinlogParser();
        r.registerEventParser(new StopEventParser());
        r.registerEventParser(new RotateEventParser());
        r.registerEventParser(new IntvarEventParser());
        r.registerEventParser(new XidEventParser());
        r.registerEventParser(new RandEventParser());
        r.registerEventParser(new QueryEventParser());
        r.registerEventParser(new UserVarEventParser());
        r.registerEventParser(new IncidentEventParser());
        r.registerEventParser(new TableMapEventParser());
        r.registerEventParser(new WriteRowsEventParser());
        r.registerEventParser(new UpdateRowsEventParser());
        r.registerEventParser(new DeleteRowsEventParser());
        r.registerEventParser(new WriteRowsEventV2Parser());
        r.registerEventParser(new UpdateRowsEventV2Parser());
        r.registerEventParser(new DeleteRowsEventV2Parser());
        r.registerEventParser(new FormatDescriptionEventParser());
        r.registerEventParser(new GtidEventParser());

        r.setStopPosition(this.stopPosition);
        r.setStartPosition(this.startPosition);
        r.setBinlogFileName(this.binlogFileName);
        r.setBinlogFilePath(this.binlogFilePath);
        return r;
    }
}
