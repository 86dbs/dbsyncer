package org.dbsyncer.storage.binlog.impl;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.file.BufferedRandomAccessFile;
import org.dbsyncer.storage.binlog.AbstractBinlogActuator;
import org.dbsyncer.storage.model.BinlogIndex;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/26 23:25
 */
public class BinlogReader extends AbstractBinlogActuator {
    private final RandomAccessFile raf;
    private final byte[] h = new byte[1];
    private byte[] b;
    private long offset;

    public BinlogReader(String path, BinlogIndex binlogIndex, long position) throws IOException {
        initBinlogIndex(binlogIndex);
        this.raf = new BufferedRandomAccessFile(new File(path + binlogIndex.getFileName()), "r");
        raf.seek(position);
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
        refreshBinlogIndexUpdateTime();
        return b;
    }

    public long getOffset() {
        return offset;
    }

    public String getFileName() {
        return binlogIndex.getFileName();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(raf);
    }
}