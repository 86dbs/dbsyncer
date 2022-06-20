package org.dbsyncer.storage.binlog;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/19 23:36
 */
public class BinlogPipeline {
    private final RandomAccessFile raf;
    private final byte[] h = new byte[1];
    private byte[] b;
    private long filePointer;

    public BinlogPipeline(RandomAccessFile raf) {
        this.raf = raf;
    }

    public byte[] readLine() throws IOException {
        this.filePointer = raf.getFilePointer();
        if (filePointer >= raf.length()) {
            return null;
        }
        raf.read(h);
        b = new byte[Byte.toUnsignedInt(h[0])];
        raf.read(b);
        raf.seek(this.filePointer + (h.length + b.length));
        return b;
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public long getFilePointer() {
        return filePointer;
    }
}