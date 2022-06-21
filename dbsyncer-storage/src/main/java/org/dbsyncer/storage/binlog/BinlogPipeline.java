package org.dbsyncer.storage.binlog;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.file.BufferedRandomAccessFile;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;

import java.io.*;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/19 23:36
 */
public class BinlogPipeline implements Closeable {
    private final RandomAccessFile raf;
    private final OutputStream out;
    private final byte[] h = new byte[1];
    private byte[] b;
    private long offset;
    private File binlogFile;

    public BinlogPipeline(File binlogFile, long pos) throws IOException {
        this.binlogFile = binlogFile;
        this.raf = new BufferedRandomAccessFile(binlogFile, "r");
        this.out = new FileOutputStream(binlogFile, true);
        raf.seek(pos);
    }

    public byte[] readLine() throws IOException {
        this.offset = raf.getFilePointer();
        if (offset >= raf.length()) {
            return null;
        }
        raf.read(h);
        b = new byte[Byte.toUnsignedInt(h[0])];
        raf.read(b);
        raf.seek(this.offset + (h.length + b.length));
        return b;
    }

    public void write(BinlogMessage message) throws IOException {
        if(null != message){
            message.writeDelimitedTo(out);
        }
    }

    public long getOffset() {
        return offset;
    }

    public String getBinlogName() {
        return binlogFile.getName();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(out);
        IOUtils.closeQuietly(raf);
    }
}