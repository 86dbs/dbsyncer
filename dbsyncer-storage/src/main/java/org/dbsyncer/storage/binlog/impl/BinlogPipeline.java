package org.dbsyncer.storage.binlog.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.storage.binlog.BinlogContext;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.model.BinlogConfig;
import org.dbsyncer.storage.model.BinlogIndex;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

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
        this.binlogReader = new BinlogReader(context.getPath(), context.getBinlogIndexByName(config.getFileName()), config.getPosition());
    }

    public void write(BinlogMessage message) throws IOException {
        binlogWriter.write(message);
    }

    public byte[] readLine() throws IOException{
        return binlogReader.readLine();
    }

    public String getFileName() {
        return binlogReader.getFileName();
    }

    public long getOffset() {
        return binlogReader.getOffset();
    }

    @Override
    public void close() {
        binlogWriter.close();
        binlogReader.close();
    }
}