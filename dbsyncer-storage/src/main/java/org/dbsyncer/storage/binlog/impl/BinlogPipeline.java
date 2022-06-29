package org.dbsyncer.storage.binlog.impl;

import org.dbsyncer.storage.binlog.proto.BinlogMessage;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/19 23:36
 */
public class BinlogPipeline implements Closeable {
    private BinlogWriter binlogWriter;
    private BinlogReader binlogReader;

    public BinlogPipeline(BinlogWriter binlogWriter, BinlogReader binlogReader) {
        this.binlogWriter = binlogWriter;
        this.binlogReader = binlogReader;
    }

    public void write(BinlogMessage message) throws IOException {
        binlogWriter.write(message);
    }

    public byte[] readLine() throws IOException {
        return binlogReader.readLine();
    }

    public long getReaderOffset() {
        return binlogReader.getOffset();
    }

    public String getReaderFileName() {
        return binlogReader.getFileName();
    }

    public String getWriterFileName() {
        return binlogWriter.getFileName();
    }

    @Override
    public void close() {
        binlogWriter.close();
        binlogReader.close();
    }

    public BinlogWriter getBinlogWriter() {
        return binlogWriter;
    }

    public void setBinlogWriter(BinlogWriter binlogWriter) {
        this.binlogWriter = binlogWriter;
    }

    public BinlogReader getBinlogReader() {
        return binlogReader;
    }

    public void setBinlogReader(BinlogReader binlogReader) {
        this.binlogReader = binlogReader;
    }
}