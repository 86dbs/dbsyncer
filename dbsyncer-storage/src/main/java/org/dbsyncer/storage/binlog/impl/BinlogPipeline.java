package org.dbsyncer.storage.binlog.impl;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.file.BufferedRandomAccessFile;
import org.dbsyncer.storage.model.BinlogIndex;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;

import java.io.*;
import java.util.List;

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
    private File file;
    private long offset;
    private List<BinlogIndex> index;

    public BinlogPipeline(List<BinlogIndex> index, File file, long pos) throws IOException {
        this.index = index;
        this.file = file;
        this.raf = new BufferedRandomAccessFile(file, "r");
        this.out = new FileOutputStream(file, true);
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
        return file.getName();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(out);
        IOUtils.closeQuietly(raf);
    }
}