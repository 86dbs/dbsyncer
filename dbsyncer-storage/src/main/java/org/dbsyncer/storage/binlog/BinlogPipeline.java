package org.dbsyncer.storage.binlog;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/19 23:36
 */
public class BinlogPipeline {
    private RandomAccessFile raf;
    private byte[] b;
    private long filePointer;

    public BinlogPipeline(RandomAccessFile raf) {
        this.raf = raf;
    }

    public byte[] readLine() throws IOException {
        this.filePointer = raf.getFilePointer();
        if (filePointer >= raf.length()) {
            b = new byte[0];
            return null;
        }
        if (b == null || b.length == 0) {
            b = new byte[(int) (raf.length() - filePointer)];
        }

        // TODO readRawVarint32
        int firstByte = raf.read(b);
        if ((firstByte & 0x80) != 0) {
            firstByte = firstByte & 0x7f;
        }

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int read = 1;
        for (int i = 1; i < firstByte; i++) {
            read++;
            stream.write(b[i]);
        }
        b = Arrays.copyOfRange(b, read, b.length);

        raf.seek(this.filePointer + read);
        byte[] _b = stream.toByteArray();
        stream.close();
        stream = null;
        return _b;
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public long getFilePointer() {
        return filePointer;
    }
}