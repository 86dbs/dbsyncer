package org.dbsyncer.storage.binlog.impl;

import org.dbsyncer.storage.binlog.BinlogContext;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.model.BinlogConfig;
import org.dbsyncer.storage.model.BinlogIndex;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/19 23:36
 */
public class BinlogPipeline implements Closeable {
    private final BinlogContext context;
    private BinlogWriter binlogWriter;
    private BinlogReader binlogReader;

    public BinlogPipeline(BinlogContext context) throws IOException {
        this.context = context;
        this.binlogWriter = new BinlogWriter(context.getPath(), context.getLastBinlogIndex());
        final BinlogConfig config = context.getConfig();
        final BinlogIndex startIndex = context.getBinlogIndexByName(config.getFileName());
        this.binlogReader = new BinlogReader(context.getPath(), startIndex, config.getPosition());
    }

    public void write(BinlogMessage message) throws IOException {
        binlogWriter.write(message);
    }

    public byte[] readLine() throws IOException{
        return binlogReader.readLine();
    }

    public long getReaderOffset() {
        return binlogReader.getOffset();
    }

    public String getReaderFileName() {
        return binlogReader.getBinlogIndex().getFileName();
    }

    public String getWriterFileName() {
        return binlogWriter.getBinlogIndex().getFileName();
    }

    @Override
    public void close() {
        binlogWriter.close();
        binlogReader.close();
    }

    public void setBinlogWriter(BinlogWriter binlogWriter) {
        this.binlogWriter = binlogWriter;
    }

    public void setBinlogReader(BinlogReader binlogReader) {
        this.binlogReader = binlogReader;
    }
}