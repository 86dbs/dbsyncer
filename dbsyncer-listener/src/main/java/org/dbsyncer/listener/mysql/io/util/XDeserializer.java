package org.dbsyncer.listener.mysql.io.util;

import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.BitColumn;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.io.XInputStream;
import org.dbsyncer.listener.mysql.io.impl.XInputStreamImpl;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class XDeserializer implements XInputStream {
    private final XInputStream tis;

    public XDeserializer(byte[] data) {
        this.tis = new XInputStreamImpl(new ByteArrayInputStream(data));
    }

    public void close() throws IOException {
        this.tis.close();
    }

    public int available() throws IOException {
        return this.tis.available();
    }

    public boolean hasMore() throws IOException {
        return this.tis.hasMore();
    }

    public void setReadLimit(int limit) throws IOException {
        this.tis.setReadLimit(limit);
    }

    public long skip(long n) throws IOException {
        return this.tis.skip(n);
    }

    public int readInt(int length) throws IOException {
        return this.tis.readInt(length);
    }

    public long readLong(int length) throws IOException {
        return this.tis.readLong(length);
    }

    public byte[] readBytes(int length) throws IOException {
        return this.tis.readBytes(length);
    }

    public BitColumn readBit(int length) throws IOException {
        return readBit(length);
    }

    public int readSignedInt(int length) throws IOException {
        return this.tis.readSignedInt(length);
    }

    public long readSignedLong(int length) throws IOException {
        return this.tis.readSignedLong(length);
    }

    public UnsignedLong readUnsignedLong() throws IOException {
        return tis.readUnsignedLong();
    }

    public StringColumn readLengthCodedString() throws IOException {
        return this.tis.readLengthCodedString();
    }

    public StringColumn readNullTerminatedString() throws IOException {
        return this.tis.readNullTerminatedString();
    }

    public StringColumn readFixedLengthString(int length) throws IOException {
        return this.tis.readFixedLengthString(length);
    }

    public int readInt(int length, boolean littleEndian) throws IOException {
        return this.tis.readInt(length, littleEndian);
    }

    public long readLong(int length, boolean littleEndian) throws IOException {
        return this.tis.readLong(length, littleEndian);
    }

    public BitColumn readBit(int length, boolean littleEndian) throws IOException {
        return tis.readBit(length, littleEndian);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return tis.read(b, off, len);
    }

    public int read() throws IOException {
        return tis.read();
    }
}
